package gen_tool_core

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"io/ioutil"
	"log"
	"os"
)

type GenTool interface {
	GetWorkMode() string
	GenMessage(record []string) (interface{} , error)
}

type GenToolCore struct {
	tool GenTool
	config GenToolConfig
	src string

	conn 		*amqp.Connection
	channel 	*amqp.Channel
	confirms	 chan amqp.Confirmation
}

func NewGenToolCore(tool GenTool) GenToolCore {

	return GenToolCore{
		tool:tool,
	}
}

func (g *GenToolCore)Init() error{

	cfg := flag.String("cfg" , "" , "")
	src := flag.String("src" , "" , "")

	flag.Parse()
	if len(*cfg) == 0|| len(*src) == 0 {
		flag.PrintDefaults()
		return errors.New("input param error")
	}

	cfgData , err := ioutil.ReadFile(*cfg)
	if err != nil {
		return err
	}

	err = json.Unmarshal(cfgData , &g.config)
	if err != nil {
		return err
	}

	if g.tool.GetWorkMode() != g.config.WorkMode {
		return errors.New("mismatch workMode")
	}

	g.src = *src
	return nil
}

func (g *GenToolCore)Run()  {
	err := g.connectMq()

	if err == nil {
		f, err := os.Open(g.src)
		if err == nil {
			defer f.Close()

			r := csv.NewReader(bufio.NewReader(f))
			index := 0
			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}

				msg ,err := g.tool.GenMessage(record)
				if err == nil {
					body , err := json.Marshal(msg)
					if index == 0 {

						var pretty bytes.Buffer
						err = json.Indent(&pretty, body, "", "\t")
						if err == nil {
							log.Println(string(pretty.Bytes()))
						}else{
							log.Println(err)
						}


						reader := bufio.NewReader(os.Stdin)
						fmt.Print("continue (Y/N): ")
						YN, _ := reader.ReadString('\n')
						fmt.Println(YN)
						if YN != "Y" {
							break
						}
						log.Println("Start Sending")
					}


					if err == nil {
						if err = g.channel.Publish(
							g.config.Exchange, // publish to an exchange
							"",           // routing to 0 or more queues
							false,        // mandatory
							false,        // immediate
							amqp.Publishing{
								Headers:         amqp.Table{},
								ContentType:     "application/json",
								ContentEncoding: "",
								Body:            []byte(body),
								DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
								Priority:        g.config.Priority,               // 0-9
								// a bunch of application/implementation-specific fields
							},
						); err != nil {
							break
						}
						confirmOne(g.confirms)
					}
					index++
				}

			}
		}

	}else {
		log.Println(err)
	}
	g.terminate()
}


func (g *GenToolCore)connectMq() error{
	connection, err := amqp.Dial(g.config.Amqp)
	if err != nil {
		return err
	}

	g.conn = connection

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return err
	}

	g.channel = channel

	log.Printf("got Channel, declaring %q Exchange (%q)", g.config.ExchangeType, g.config.Exchange)
	err = channel.ExchangeDeclare(
		g.config.Exchange,     // name
		g.config.ExchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return err
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.

	log.Printf("enabling publishing confirms.")
	if err := channel.Confirm(false); err != nil {
		if err != nil {
			return  err
		}
	}

	g.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil

}


func (g *GenToolCore)terminate(){
	if g.channel != nil {
		g.channel.Close()
	}

	if g.conn != nil {
		g.conn.Close()
	}
}


func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}