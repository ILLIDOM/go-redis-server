package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
)

type RedisMessageType byte

const (
	SimpleString RedisMessageType = '+'
	BulkString   RedisMessageType = '$'
	Array        RedisMessageType = '*'
)

type RedisMessage struct {
	typ   RedisMessageType // type of message (SimpleString, BulkString, Array)
	bytes []byte
	array []RedisMessage
}

func (rm RedisMessage) String() string {
	if rm.typ == SimpleString || rm.typ == BulkString {
		return string(rm.bytes)
	}
	return ""
}

func (rm RedisMessage) Array() []RedisMessage {
	if rm.typ == Array {
		return rm.array
	}
	return []RedisMessage{}
}

func handleRedisMessage(stream *bufio.Reader) (RedisMessage, error) {
	dataType, err := stream.ReadByte()
	if err != nil {
		fmt.Println("Error while reading dataType")
		return RedisMessage{}, err
	}

	switch dataType {
	case byte(SimpleString): // simple string '+'
		return parseSimpleString(stream)
	case byte(BulkString): // Bulk String '$'
		return parseBulkString(stream)
	case byte(Array): // Array '*'
		return parseArray(stream)
	}

	return RedisMessage{}, nil
}

// e.g. "PING\r\n"
func parseSimpleString(stream *bufio.Reader) (RedisMessage, error) {
	message, err := stream.ReadBytes('\n') // PING\r\n
	if err != nil {
		fmt.Println("Error reading SimpleString")
		return RedisMessage{}, err
	}
	return RedisMessage{
		typ:   SimpleString,
		bytes: message[:len(message)-2],
	}, nil
}

// e.g. "4\r\nECHO\r\n"
func parseBulkString(stream *bufio.Reader) (RedisMessage, error) {
	stringLengthBytes, err := stream.ReadByte()
	if err != nil {
		fmt.Println("Error reading BulkString length")
		return RedisMessage{}, err
	}

	// skip \r\n at end of string
	io.CopyN(ioutil.Discard, stream, 2)

	stringLengthInt, err := strconv.Atoi(string(stringLengthBytes))
	if err != nil {
		fmt.Println("Error parsing string length")
		return RedisMessage{}, err
	}

	buffer := make([]byte, stringLengthInt)
	for i := 0; i < stringLengthInt; i++ { //could use .ReadFull(buffer) instead of loop
		n, err := stream.ReadByte()
		if err != nil {
			fmt.Println("Error reading BulkString")
			return RedisMessage{}, err
		}
		buffer[i] = n
	}
	// skip \r\n at end of string
	io.CopyN(ioutil.Discard, stream, 2)

	return RedisMessage{
		typ:   BulkString,
		bytes: buffer,
	}, nil
}

// e.g. 2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
func parseArray(stream *bufio.Reader) (RedisMessage, error) {
	arrayLengthBytes, err := stream.ReadByte() // read array length //2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
	if err != nil {
		fmt.Println("Error reading array length")
		return RedisMessage{}, err
	}

	// discard \r\n
	io.CopyN(ioutil.Discard, stream, 2)

	arrayLengthInt, err := strconv.Atoi(string(arrayLengthBytes))
	if err != nil {
		fmt.Println("Error parsing array length")
		return RedisMessage{}, err
	}

	redisArray := []RedisMessage{}

	for i := 0; i < arrayLengthInt; i++ {
		redisMessage, err := handleRedisMessage(stream)
		if err != nil {
			return RedisMessage{}, nil
		}
		redisArray = append(redisArray, redisMessage)
	}

	return RedisMessage{
		typ:   Array,
		array: redisArray,
	}, nil

}
