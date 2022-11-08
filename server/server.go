package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func handleConnection(conn net.Conn, storage *Storage) {
	defer conn.Close()
	fmt.Println("Accepted connection:")

	for {
		fmt.Println("Start reading request....")

		redisMessage, err := handleRedisMessage(bufio.NewReader(conn))
		if err != nil {
			fmt.Println("Unable to read from connection")
			return
		}

		redisCommand := redisMessage.Array()[0].String()
		redisArguments := redisMessage.Array()[1:]

		switch redisCommand {
		case "ping":
			fmt.Printf("Sending answer to client\n")
			conn.Write([]byte("+PONG\r\n"))
		case "echo":
			fmt.Printf("Sending answer to client\n")
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(redisArguments[0].String()), redisArguments[0].String())))
		case "set":
			if len(redisArguments) < 2 {
				fmt.Printf("Can't SET key -> wrong arguments\n")
				conn.Write([]byte("-ERR INVALID ARGUMENTS FOR COMMAND SET"))
			}

			var err error

			if len(redisArguments) == 2 {
				// no expiration given
				err = storage.SetKey(redisArguments[0].String(), redisArguments[1].String())
			} else {
				// expiration time is in arguments[3]
				err = storage.SetKeyWithExpiration(redisArguments[0].String(), redisArguments[1].String(), redisArguments[3].String())
			}

			if err != nil {
				fmt.Printf("Sending answer to client\n")
				conn.Write([]byte("-ERR - server was not able to set key"))
			}

			fmt.Printf("Sending answer to client\n")
			conn.Write([]byte("+OK\r\n"))
		case "get":
			if len(redisArguments) < 1 {
				fmt.Printf("Can' GET key -> wrong arguments\n")
				fmt.Printf("Sending answer to client\n")
				conn.Write([]byte("-ERR INVALID ARGUMENTS FOR COMMAND GET"))
			}
			value, err := storage.GetValue(redisArguments[0].String())
			if err != nil {
				fmt.Printf("%v", err)
				fmt.Printf("Sending answer to client\n")
				conn.Write([]byte("$-1\r\n"))
			}
			fmt.Printf("Sending answer to client\n")
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
		default:
			fmt.Printf("Sending answer to client\n")
			conn.Write([]byte(fmt.Sprintf("-ERR INVALID COMMAND %s\r\n", redisCommand)))
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	// initialize in-memory datastructure to hold key-value pairs
	storage := InitStorage()

	fmt.Println("Server listening...")

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleConnection(c, storage)
	}
}
