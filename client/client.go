package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	serverIP := "192.168.0.199"
	serverPort := "6379"

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%s", serverIP, serverPort))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Sending message")

	message := "*3\r\n$3\r\nset\r\n$4\r\nheya\r\n$4\r\nheya\r\n" // Array of RESP Bulk String
	c.Write([]byte(message))

	reply, err := bufio.NewReader(c).ReadString(byte('\n'))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Received from Server: %s\n", reply)

}
