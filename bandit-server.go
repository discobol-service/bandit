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

// Arm - ...
type Arm struct {
	Arm    string `json:"arm"`
	Reward float64
	Hits   int
	Domain string
}

// Stat - ...
type Stat struct {
	Arm    string
	Hits   int
	Reward float64
	Scores float64
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
	router.POST("/stat/list/:domain", postStatList)

	router.Run(":4444")
}

// curl -X POST -d '["80C300A59541", "14CB94CD2226"]' http://localhost:4444/stat/list/default
func postStatList(c *gin.Context) {
	var arms []string
	if err := c.ShouldBindJSON(&arms); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

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

		select arm, hits, reward
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
		rows.Scan(&stat.Arm, &stat.Hits, &stat.Reward)

		totalHits = totalHits + stat.Hits
		stats = append(stats, *stat)
	}

	// Посчитать скоры, упорядочить массив
	var scores = make([]StatResponse, 0, 0)
	for _, s := range stats {
		var statResp = new(StatResponse)

		statResp.Scores = math.Sqrt((2.0*math.Log(float64(totalHits)))/float64(s.Hits)) + s.Reward
		statResp.Arm = s.Arm

		log.Printf("stat: %+v\n", s)
		scores = append(scores, *statResp)
	}
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Scores > scores[j].Scores
	})

	return scores, nil
}

// curl -X POST -d '{"arm":"14CB94CD2226", "hits":1}' http://localhost:4444/hits/default
func postHits(c *gin.Context) {
	var req Arm
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	arm, err := storage.GetArm(req.Arm, c.Param("domain"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err = arm.UpdateHits(req.Hits); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"message": "ok"})
}

// UpdateHits - ...
func (a Arm) UpdateHits(hits int) error {
	sql := `

		update bandit_stat 
			set hits = hits + $1
		  where arm = $2 
			and domain = $3
	
	`

	_, err := storage.db.Exec(context.Background(), sql, hits, a.Arm, a.Domain)
	return err
}

// GetArm - ...
func (s *StorageManager) GetArm(arm, domain string) (Arm, error) {
	log.Println("arm = " + arm)
	log.Println("domain = " + domain)

	var a Arm
	sql := `
	
		select hits, reward, domain
		from bandit_stat
		where arm = $1 and domain = $2

	`

	if err := s.db.QueryRow(context.Background(), sql, arm, domain).Scan(&a.Hits, &a.Reward, &a.Domain); err != nil {
		return a, err
	}
	a.Arm = arm

	return a, nil
}

func postReward(c *gin.Context) {

	log.Println("POST Reward")
	var req Arm
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	arm, err := storage.GetArm(req.Arm, c.Param("domain"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err = arm.UpdateReward(req.Reward); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"message": "ok"})
}

// UpdateReward - ...
func (a Arm) UpdateReward(reward float64) error {
	sql := `
	
		update bandit_stat
		   set reward = $1
		 where arm = $2 and domain = $3 
	
	`
	var newReward float64
	newReward = (a.Reward*(float64(a.Hits)-1) + reward) / float64(a.Hits)

	_, err := storage.db.Exec(context.Background(), sql, newReward, a.Arm, a.Domain)
	return err
}
