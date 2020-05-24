package main

// Бандит использует постгрес для хранения данных, штош, так удобнее и быстрее.
// Ну и я не вижу особых оснований вводить какие-то новые сущности - вот когда
// у нас будет хайлоад, тогда и переделаем на что-то другое.

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx"
)

// StorageManager - ...
type StorageManager struct {
	db *pgx.Conn
}

var storage *StorageManager

// ArmRequest - ...
type ArmRequest struct {
	Arm    string `json:"arm"`
	Reward float64
	Hits   int
}

// Stat - ...
type Stat struct {
	Arm     string
	Hits    int
	Rewards []float64
	Scores  float64
}

// StatResponse - ...
type StatResponse struct {
	Arm    string
	Scores float64
}

func init() {
	conn, err := pgx.Connect(context.Background(), "postgresql://shootnix:12345@localhost/discobol")
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", err.Error())
		os.Exit(1)
	}
	storage = new(StorageManager)
	storage.db = conn
}

func main() {

	defer storage.db.Close(context.Background())
	router := gin.Default()

	router.POST("/hits/:domain", postHits)
	router.POST("/reward/:domain", postReward)
	router.POST("/stat/list/:domain", statList)

	router.Run()
}

func statList(c *gin.Context) {
	var arms []string
	if err := c.ShouldBindJSON(&arms); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	log.Println(arms)

	if len(arms) == 0 {
		c.JSON(http.StatusNoContent, gin.H{"message": "no arms - no stats"})
		return
	}

	statList, err := storage.GetStat(arms, c.Param("domain"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, statList)
}

// GetStat - ...
func (s *StorageManager) GetStat(arms []string, domain string) ([]StatResponse, error) {
	stats := make([]Stat, 0, 0)

	sql := `

		select arm, hits, rewards
		  from bandit_stat
		 where domain = $1 and 
			   arm in (%s) and 
			   hits != 0

	`

	binds := make([]interface{}, len(arms)+1)
	binds[0] = domain
	placeHolders := []string{}
	i := 2
	for k, arm := range arms {
		binds[k+1] = arm
		placeHolders = append(placeHolders, "$"+strconv.Itoa(i))
		i = i + 1
	}
	inStr := strings.Join(placeHolders[:], ", ")
	rows, err := s.db.Query(context.Background(), fmt.Sprintf(sql, inStr), binds...)
	if err != nil {
		return nil, err
	}

	totalHits := 0
	for rows.Next() {
		var stat = new(Stat)
		rows.Scan(&stat.Arm, &stat.Hits, &stat.Rewards)

		totalHits = totalHits + stat.Hits
		stats = append(stats, *stat)
	}

	// Посчитать скоры, упорядочить массив
	var scores = make([]StatResponse, 0, 0)
	for _, s := range stats {
		var statResp = new(StatResponse)

		var rewards float64
		if len(s.Rewards) > 0.0 {
			for _, r := range s.Rewards {
				rewards = rewards + r
			}
			rewards = rewards / float64(len(s.Rewards))
		}

		statResp.Scores = math.Sqrt((2.0*math.Log(float64(totalHits)))/float64(s.Hits)) + rewards
		statResp.Arm = s.Arm

		log.Printf("stat: %+v\n", s)
		scores = append(scores, *statResp)
	}
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Scores > scores[j].Scores
	})

	return scores, nil
}

func postHits(c *gin.Context) {
	var req ArmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Println("Arm = " + req.Arm)
	if err := storage.Incr(req.Arm, c.Param("domain"), req.Hits); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"message": "ok"})
}

// Incr - ...
func (s *StorageManager) Incr(arm, domain string, hits int) error {
	isNewArm, err := s.checkArm(arm, domain)
	if err != nil {
		return err
	}

	if isNewArm {
		s.insertHits(arm, domain, hits)
	} else {
		s.updateHits(arm, domain, hits)
	}

	return nil
}

func postReward(c *gin.Context) {

	log.Println("POST Reward")
	var req ArmRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	//log.Println("Arm = " + req.Arm)
	if err := storage.AddReward(req.Arm, c.Param("domain"), req.Reward); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"message": "ok"})
}

func (s *StorageManager) checkArm(arm, domain string) (bool, error) {
	sql := `
	
		select true from bandit_stat
		where arm = $1 and domain = $2
	
	`

	row := s.db.QueryRow(context.Background(), sql, arm, domain)
	var existed bool
	row.Scan(&existed)

	log.Println(existed)

	return !existed, nil
}

func (s *StorageManager) insertHits(arm, domain string, hits int) error {
	sql := `

		insert into bandit_stat (arm, domain, hits)
		values ($1, $2, $3)
	
	`

	if _, err := storage.db.Exec(context.Background(), sql, arm, domain, hits); err != nil {
		return err
	}

	return nil
}

func (s *StorageManager) updateHits(arm, domain string, hits int) error {
	sql := `

		update bandit_stat 
			set hits = hits + $1
		  where arm = $2 
			and domain = $3
	
	`

	_, err := storage.db.Exec(context.Background(), sql, hits, arm, domain)
	return err
}

// AddReward - ...
func (s *StorageManager) AddReward(arm, domain string, reward float64) error {
	log.Println(">>> AddReward")

	isNewArm, err := s.checkArm(arm, domain)
	if err != nil {
		return err
	}

	if isNewArm {
		if err := s.insertReward(arm, domain, reward); err != nil {
			return err
		}
	} else {
		if err := s.updateReward(arm, domain, reward); err != nil {
			return err
		}
	}

	return nil
}

func (s *StorageManager) insertReward(arm, domain string, reward float64) error {
	sql := `
	
		insert into bandit_stat (arm, domain, hits, rewards)
		values ($1, $2, $3, $4)
	
	`
	_, err := storage.db.Exec(context.Background(), sql, arm, domain, 1, reward)
	return err
}

func (s *StorageManager) updateReward(arm, domain string, reward float64) error {
	sql := `
	
		update bandit_stat
		   set rewards = array_append(rewards, $1)
		 where arm = $2 and domain = $3 
	
	`
	_, err := storage.db.Exec(context.Background(), sql, reward, arm, domain)
	return err
}
