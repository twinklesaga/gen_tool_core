package gen_tool_core

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type GenTool interface {
	GetWorkMode() string
	GenMessage(index int , record []string) (interface{} , error)
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

			delim := ","
			if len(g.config.Delim) > 0 {
				delim = g.config.Delim
			}

			scanner := bufio.NewScanner(f)
			index := 0
			for scanner.Scan(){

				line := scanner.Text()

				record := strings.Split(line , delim)

				if len(record) <= g.config.RecordLen {
					log.Printf("Record Error : len(%d) %v\n" , g.config.RecordLen, record)
					break
				}
				if record[0] != g.config.WorkMode {
					log.Printf("WorkMode Error : %s != %s\n" , record[0] , g.config.WorkMode)
					break
				}
				if strings.HasPrefix(record[0] , "!") {
					log.Printf("Skip %v \n" , record)
					continue
				}

				msg ,err := g.tool.GenMessage(index , record)

				if err == nil {
					body , err := json.Marshal(msg)
					if index == 0 {
						fmt.Printf("mq : %s\n" , g.config.Amqp)
						fmt.Printf("     %s , %s\n" , g.config.Exchange , g.config.ExchangeType)
						fmt.Printf("source : %s\n", g.src)
						fmt.Printf("record : %v" , record)


						var pretty bytes.Buffer
						err = json.Indent(&pretty, body, "", "    ")
						if err == nil {
							fmt.Println(string(pretty.Bytes()))
						}else{
							log.Println(err)
						}

						reader := bufio.NewReader(os.Stdin)
						fmt.Print("continue (Y/N): ")
						YN, _ := reader.ReadString('\n')
						if strings.Compare(YN[0:1] , "Y") != 0{
							log.Println("Stop Sending")
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
				} else {
					log.Println(err , index , recover())
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

	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	g.channel = channel

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
	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}