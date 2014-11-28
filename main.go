package main

import (
	"flag"
	"fmt"
	"time"
	"strconv"
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

func makeInterfaceSlice(data string) []interface{} {
	var interfaceSlice []interface{} = make([]interface{}, 1)
	interfaceSlice[0] = data

	return interfaceSlice
}

func main() {
	redisPool := SetupRedis()
	defer redisPool.Close()

	c := redisPool.Get()
	defer c.Close()

	db, err := sql.Open("postgres", "host=se-sms-1 user=hibernate dbname=smstest3 password=Cegthgfhjkm! sslmode=disable ")
	PanicIf(err)

	now := time.Now()

	// Get outgoing messages uniqueid,messagestatus and addressee fullname. So, we need here all data for columns from journal.
	rows, err := db.Query("SELECT outm.uniqueid, outm.messagestatus, addr.fullname FROM outgoingmessage outm " +
			"JOIN addressee addr ON outm.addressee = addr.uniqueid ")
	PanicIf(err)
	defer rows.Close()

	// Start transaction
	c.Send("MULTI")

	var i int = 0
	for rows.Next() {
		var uniqueid string
		var messagestatus string
		var addresse_fullname string
		err = rows.Scan(&uniqueid, &messagestatus, &addresse_fullname)

		// Produce all data from postgresql to redis
		c.Send("SADD", messagestatus, uniqueid)
		c.Send("SADD", addresse_fullname, uniqueid)

		i++
//		fmt.Println(uniqueid + " : " + messagestatus)
	}
	err = rows.Err() // get any error encountered during iteration

	// Commit transaction
	_, err = c.Do("EXEC")
	PanicIf(err)

	then := time.Now()
	diff := then.Sub(now)
	fmt.Println("Proceed " + strconv.Itoa(i) + " rows from DB in " + strconv.FormatFloat(diff.Seconds(), 'g', -1, 64) + " seconds")

	// TEST: Get all messages in some status
	values, err := redis.Strings(c.Do("SMEMBERS", "bf5bc07c-ac9d-45a5-9ef4-1556be6891e4"))
	PanicIf(err)

	fmt.Println("Found " + strconv.Itoa(len(values)) + " values from record 'bf5bc07c-ac9d-45a5-9ef4-1556be6891e4'")

	// TEST: Try to get all addresses by fullname pattern
	keys, err := redis.Strings(c.Do("KEYS", "*Свирид*"))
	PanicIf(err)

	var got_keys []interface {}
	got_keys = append(got_keys, "result") // TODO: Result must be unique and temp name
	for _,key := range keys {
		got_keys = append(got_keys, key)

		values, err := redis.Strings(c.Do("SMEMBERS", key))
		PanicIf(err)

		fmt.Println("Found " + strconv.Itoa(len(values)) + " values from record '" + key +"'")
	}

	// Union all founded addresses into one set
	result, err := redis.Int(c.Do("SUNIONSTORE", got_keys...))
	PanicIf(err)
	fmt.Println("Union " + strconv.Itoa(result) + " values from previosly founded records")

	// Then intersect one of message status and result - we will get message for found addresses with current status
	intersects, err := redis.Strings(c.Do("SINTER", "bf5bc07c-ac9d-45a5-9ef4-1556be6891e4", "result"))
	PanicIf(err)

	fmt.Println("Intersect " + strconv.Itoa(len(intersects)) + " values from record 'bf5bc07c-ac9d-45a5-9ef4-1556be6891e4' and union")
}
