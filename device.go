package tbusmqtt

import (
	"bytes"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/robotalks/tbus/go/tbus"
)

type topicDevice struct {
	conn     *Connector
	topic    string
	uniqueID string
	port     tbus.BusPort
	info     tbus.DeviceInfo
	init     chan error
}

func (d *topicDevice) DispatchMsg(msg *tbus.Msg) error {
	if msg.Head.MsgID != nil {
		encodedID := d.uniqueID + "/" + hex.EncodeToString(msg.Head.MsgID)
		msg.Head.MsgID = []byte(encodedID)
	}
	token := d.conn.publishCtlMsg(d.topic, msg)
	token.Wait()
	return token.Error()
}

func (d *topicDevice) AttachTo(port tbus.BusPort, addr uint8) {
	d.port = port
	d.info.Address = uint32(addr)
}

func (d *topicDevice) BusPort() tbus.BusPort {
	return d.port
}

func (d *topicDevice) DeviceInfo() tbus.DeviceInfo {
	return d.info
}

func (d *topicDevice) subscribe() mqtt.Token {
	respTopic := d.conn.TopicBase + "/" + d.topic + "/r/" + d.uniqueID + "/#"
	eventTopic := d.conn.TopicBase + "/" + d.topic
	return d.conn.Client.SubscribeMultiple(map[string]byte{
		respTopic:  0,
		eventTopic: 0,
	}, d.handleMessage)
}

func (d *topicDevice) connect() error {
	d.init = make(chan error, 1)
	defer func() {
		close(d.init)
		d.init = nil
	}()
	token := d.subscribe()
	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}
	msg := tbus.BuildMsg().MsgID([]byte(d.uniqueID+"/0")).EncodeBody(0, nil).Build()
	token = d.conn.publishCtlMsg(d.topic, msg)
	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}
	select {
	case <-time.After(d.conn.Timeout):
		return tbus.ErrRecvTimeout
	case err := <-d.init:
		return err
	}
}

func (d *topicDevice) disconnect() error {
	respTopic := d.conn.TopicBase + "/" + d.topic + "/r/" + d.uniqueID + "/#"
	eventTopic := d.conn.TopicBase + "/" + d.topic
	token := d.conn.Client.Unsubscribe(respTopic, eventTopic)
	token.Wait()
	return token.Error()
}

func (d *topicDevice) recvDeviceInfo(topicMsg mqtt.Message) (bool, error) {
	if topicMsg.Topic() != d.conn.TopicBase+"/"+d.topic+"/r/"+d.uniqueID+"/0" {
		return false, nil
	}
	msg, err := tbus.Decode(bytes.NewBuffer(topicMsg.Payload()))
	if err != nil {
		log.Printf("device %s ctl %s decode message failed: %v", d.topic, d.uniqueID, err)
		return false, err
	}
	var info tbus.DeviceInfo
	if err = msg.Body.Decode(&info); err != nil {
		return true, err
	}
	info.Address = d.info.Address
	d.info = info
	return true, nil
}

func (d *topicDevice) handleMessage(_ mqtt.Client, topicMsg mqtt.Message) {
	if errCh := d.init; errCh != nil {
		ok, err := d.recvDeviceInfo(topicMsg)
		if ok {
			errCh <- err
		}
		return
	}
	topic := topicMsg.Topic()
	if topic != d.conn.TopicBase+"/"+d.topic ||
		!strings.HasPrefix(topic, d.conn.TopicBase+"/"+d.topic+"/r/"+d.uniqueID+"/") {
		// not our message
		return
	}
	msg, err := tbus.Decode(bytes.NewBuffer(topicMsg.Payload()))
	if err != nil {
		log.Printf("device %s ctl %s decode message failed: %v", d.topic, d.uniqueID, err)
	} else if port := d.port; port != nil {
		port.DispatchMsg(&msg)
	}
}
