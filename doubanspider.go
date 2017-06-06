package main

import (
	"database/sql"
	"log"
	"regexp"
	"strconv"

	redis "gopkg.in/redis.v5"

	"fmt"

	"strings"

	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
)

// RedisSubjectListKey RedisSubjectListKey
const RedisSubjectListKey = "douban:book:subject:ids"

// RedisSubjectSetKey RedisSubjectSetKey
const RedisSubjectSetKey = "douban:book:subject:set:ids"

func spider(subID int64, db *sql.DB, redisClient *redis.Client) {
	url := "https://book.douban.com/subject/" + strconv.FormatInt(subID, 10)
	fmt.Println(url)
	doc, err := goquery.NewDocument(url)
	if err != nil {
		log.Panic(err)
	}
	stmt, _ := db.Prepare("insert into subject(id, name, rating, content) values(?, ?, ?, ?)")
	defer stmt.Close()
	doc.Find("#wrapper").Each(func(i int, s *goquery.Selection) {
		subName := s.Find("h1 span").Text()
		ratingNum, _ := strconv.ParseFloat(strings.Trim(s.Find(".rating_num").Text(), " "), 64)
		content, _ := s.Find(".intro").Eq(1).Html()
		_, err := stmt.Exec(subID, subName, ratingNum, content)
		if err != nil {
			log.Panic(err)
		}
		s.Find("#content #db-rec-section .content").Find("a").Each(func(i int, s *goquery.Selection) {
			href, _ := s.Attr("href")
			subIDToken, _ := regexp.Compile("https://book.douban.com/subject/(?P<loginToken>\\w+)")
			subIDTokens := subIDToken.FindStringSubmatch(href)
			var newSubID string
			if subIDTokens != nil {
				newSubID = subIDTokens[1]
				spidered, err := strconv.ParseBool(redisClient.SIsMember(RedisSubjectSetKey, newSubID).String())
				if err != nil {
					log.Panic(err)
				}
				fmt.Printf(strconv.FormatBool(spidered))
				if !spidered {
					redisClient.RPush(RedisSubjectListKey, newSubID)
				}
			}
		})
	})
	redisClient.SAdd(RedisSubjectSetKey, subID)
}

func start(db *sql.DB, redisClient *redis.Client) {
	subID, err := redisClient.LPop(RedisSubjectListKey).Int64()
	for err == nil {
		go spider(subID, db, redisClient)
		subID, err = redisClient.LPop(RedisSubjectListKey).Int64()
	}
	time.Sleep(2)
	start(db, redisClient)
}

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(192.168.1.6:3306)/douban")
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer redisClient.Close()
	_, err = redisClient.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	start(db, redisClient)
	ch := make(chan int, 1)
	ch <- 1
	ch <- 2
}
