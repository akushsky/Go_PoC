package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/codegangsta/martini"
	"github.com/garyburd/redigo/redis"
	"github.com/martini-contrib/render"
)

var (
	redisAddress   = flag.String("redis-address", ":6379", "Address to the Redis server")
	maxConnections = flag.Int("max-connections", 10, "Max connections to Redis")
)

func main() {
	martini.Env = martini.Prod

	flag.Parse()

	redisPool := redis.NewPool(func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", *redisAddress)

			if err != nil {
				return nil, err
			}

			return c, err
		}, *maxConnections)

	defer redisPool.Close()

	m := martini.Classic()

	m.Map(redisPool)

	m.Use(render.Renderer())

	m.Get("/", func() string {
			return "Hello from Martini!"
		})

	m.Get("/set/:key", func(r render.Render, pool *redis.Pool, params martini.Params, req *http.Request) {
			key := params["key"]
			value := req.URL.Query().Get("value")

			c := pool.Get()
			defer c.Close()

			status, err := c.Do("SET", key, value)

			if err != nil {
				message := fmt.Sprintf("Could not SET %s:%s", key, value)

				r.JSON(400, map[string]interface{}{
					"status":  "ERR",
					"message": message})
			} else {
				r.JSON(200, map[string]interface{}{
					"status": status})
			}
		})

	m.Get("/get/:key", func(r render.Render, pool *redis.Pool, params martini.Params) {
			key := params["key"]

			c := pool.Get()
			defer c.Close()

			value, err := redis.String(c.Do("GET", key))

			if err != nil {
				message := fmt.Sprintf("Could not GET %s", key)

				r.JSON(400, map[string]interface{}{
					"status":  "ERR",
					"message": message})
			} else {
				r.JSON(200, map[string]interface{}{
					"status": "OK",
					"value":  value})
			}
		})

	m.Run()
}
