package tbusmqtt

import (
	"bytes"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	tbus "github.com/robotalks/tbus/go/tbus"
)

type busPort struct {
	conn   *Connector
	device tbus.Device
	topic  string
}

// DispatchMsg implements BusPort
func (p *busPort) DispatchMsg(msg *tbus.Msg) error {
	topic := p.topic
	if msg.Head.MsgID != nil {
		topic += "/r/" + string(msg.Head.MsgID)
	}
	token := p.conn.publishMsg(topic, msg)
	token.Wait()
	return token.Error()
}

func (p *busPort) subscribe() mqtt.Token {
	return p.conn.subscribeCtl(p.topic, p.handleMessage)
}

func (p *busPort) subscribeAndWait() error {
	token := p.subscribe()
	token.Wait()
	return token.Error()
}

func (p *busPort) unsubscribe() mqtt.Token {
	return p.conn.unsubscribeCtl(p.topic)
}

func (p *busPort) handleMessage(_ mqtt.Client, topicMsg mqtt.Message) {
	msg, err := tbus.Decode(bytes.NewBuffer(topicMsg.Payload()))
	if err != nil {
		log.Printf("device %s decode message failed: %v", p.topic, err)
	} else if dev := p.device; dev != nil {
		dev.DispatchMsg(&msg)
	}
}
