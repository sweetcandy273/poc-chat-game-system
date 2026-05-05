package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ctx = context.Background()

// ===== Redis =====
var rdb = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

// ===== Mongo =====
var mongoClient *mongo.Client
var messageCollection *mongo.Collection

// ===== Struct =====
type Client struct {
	conn  *websocket.Conn
	rooms map[string]bool
}

type IncomingMessage struct {
	Action   string   `json:"action"`
	Rooms    []string `json:"rooms"`
	Text     string   `json:"text"`
	Username string   `json:"username"`
}

// ===== Global =====
var clients = make(map[*Client]bool)

// ===== INIT Mongo =====
func initMongo() {
	var err error

	mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("Mongo connect error:", err)
	}

	messageCollection = mongoClient.Database("chatdb").Collection("messages")

	log.Println("Mongo connected")
}

// ===== MAIN =====
func main() {
	app := fiber.New()

	initMongo()

	app.Static("/", "./public")

	go subscribeRedis()

	app.Get("/ws", websocket.New(func(c *websocket.Conn) {

		client := &Client{
			conn:  c,
			rooms: make(map[string]bool),
		}
		clients[client] = true

		defer func() {
			delete(clients, client)
			c.Close()
		}()

		for {
			var msg IncomingMessage
			if err := c.ReadJSON(&msg); err != nil {
				break
			}

			switch msg.Action {

			// ===== JOIN =====
			case "join":
				for _, r := range msg.Rooms {
					client.rooms[r] = true
				}

			// ===== SEND =====
			case "send":
				for _, r := range msg.Rooms {

					// 🔥 1. Save to Mongo
					_, err := messageCollection.InsertOne(ctx, map[string]interface{}{
						"room":     r,
						"text":     msg.Text,
						"username": msg.Username,
						"created":  time.Now(),
					})
					if err != nil {
						log.Println("Mongo insert error:", err)
					}

					// 🔥 2. Publish to Redis
					payload, _ := json.Marshal(map[string]string{
						"room":     r,
						"text":     msg.Text,
						"username": msg.Username,
					})

					rdb.Publish(ctx, "room:"+r, payload)
				}
			}
		}
	}))

	log.Println("Server running: http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}

// ===== Redis Subscribe =====
func subscribeRedis() {
	pubsub := rdb.PSubscribe(ctx, "room:*")
	ch := pubsub.Channel()

	for msg := range ch {

		var data map[string]string
		json.Unmarshal([]byte(msg.Payload), &data)

		room := data["room"]

		for client := range clients {
			if client.rooms[room] {
				err := client.conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
				if err != nil {
					client.conn.Close()
					delete(clients, client)
				}
			}
		}
	}
}
