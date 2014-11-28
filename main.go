package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/lib/pq"
	"database/sql"
)

var (
	redisAddress   = flag.String("redis-address", "127.0.0.1:6379", "Address to the Redis server")
	maxConnections = flag.Int("max-connections", 10, "Max connections to Redis")
)

func SetupRedis() *redis.Pool {
	redisPool := redis.NewPool(func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", *redisAddress)

			PanicIf(err)

			return c, err
		}, *maxConnections)

	return redisPool
}

func PanicIf(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	db, err := sql.Open("postgres", "host=se-sms-1 user=hibernate dbname=smstest3 password=Cegthgfhjkm! sslmode=disable ")
	PanicIf(err)

	rows, err := db.Query("SELECT uniqueid FROM daytype WHERE isrelevant=true")
	PanicIf(err)
	defer rows.Close()

	for rows.Next() {
		var uniqueid string
		err = rows.Scan(&uniqueid)

		fmt.Println(uniqueid)
	}
	err = rows.Err() // get any error encountered during iteration

	redisPool := SetupRedis()

	c := redisPool.Get()
	defer c.Close()

	c.Do("SET", "a", "b")
	value, err := redis.String(c.Do("GET", "a"))

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(value)
	}

	defer redisPool.Close()
}
