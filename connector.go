package tbusmqtt

import (
	"bytes"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	tbus "github.com/robotalks/tbus/go/tbus"
	shortid "github.com/ventu-io/go-shortid"
)

// Connector is MQTT connector
type Connector struct {
	Client    mqtt.Client
	TopicBase string
	Timeout   time.Duration

	onConnectHandler mqtt.OnConnectHandler
	ports            map[string]*busPort
	devices          map[string]*topicDevice
	lock             sync.RWMutex
}

// NewConnectorClient creates a new connector with existing client
// note: the pre-registered ConnectHandler should call Connector.Connected()
// to re-subscribe topics
func NewConnectorClient(topicBase string, client mqtt.Client) *Connector {
	conn := &Connector{
		Client:    client,
		TopicBase: topicBase,
		Timeout:   tbus.DefaultInvocationTimeout,
		ports:     make(map[string]*busPort),
		devices:   make(map[string]*topicDevice),
	}
	return conn
}

// NewConnectorClientOptions creates a new connector with ClientOptions
// and returns an unconnected Connector.
// ConnectHandler is automatically chained so Connector will take care of
// re-subscribe topics during reconnection
// it's recommended to enable reconnect in options
// Use Connector.Client.Connect() to initiate the connection
func NewConnectorClientOptions(topicBase string, options *mqtt.ClientOptions) *Connector {
	conn := &Connector{
		TopicBase:        topicBase,
		onConnectHandler: options.OnConnect,
		ports:            make(map[string]*busPort),
	}
	options.OnConnect = conn.onConnect
	conn.Client = mqtt.NewClient(options)
	return conn
}

// Connected is a callback function to re-subscribe topics during reconnection
func (c *Connector) Connected() {
	tokens := make(map[string]mqtt.Token)
	c.lock.RLock()
	if c.ports != nil {
		for topic, bp := range c.ports {
			tokens["device topic "+topic] = bp.subscribe()
		}
	}
	if c.devices != nil {
		for _, dev := range c.devices {
			tokens["device topic "+dev.topic+" ctl "+dev.uniqueID] = dev.subscribe()
		}
	}
	c.lock.RUnlock()
	for topic, token := range tokens {
		token.Wait()
		if err := token.Error(); err != nil {
			log.Printf("%s subscription failed: %v", topic, err)
		}
	}
}

// ConnectDevice connects a device
func (c *Connector) ConnectDevice(subTopic string) (tbus.Device, error) {
	dev := &topicDevice{
		conn:     c,
		topic:    subTopic,
		uniqueID: shortid.MustGenerate(),
	}
	c.lock.Lock()
	if c.devices == nil {
		c.devices = make(map[string]*topicDevice)
	}
	c.devices[dev.uniqueID] = dev
	c.lock.Unlock()
	err := dev.connect()
	if err != nil {
		c.DisconnectDevice(dev)
		return nil, err
	}
	return dev, nil
}

// DisconnectDevice disconnects a device
func (c *Connector) DisconnectDevice(dev tbus.Device) error {
	topicDev := dev.(*topicDevice)
	if err := topicDev.disconnect(); err != nil {
		return err
	}
	c.lock.Lock()
	if c.devices != nil {
		delete(c.devices, topicDev.uniqueID)
	}
	c.lock.Unlock()
	return nil
}

// Expose a device

// Attach attaches a device to a topic on MQTT
func (c *Connector) Attach(subTopic string, dev tbus.Device) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.ports == nil {
		c.ports = make(map[string]*busPort)
	}
	bp := c.ports[subTopic]
	if bp != nil {
		bp.device.AttachTo(nil, 0)
		bp.device = dev
	} else {
		bp = &busPort{conn: c, device: dev, topic: subTopic}
		if err := bp.subscribeAndWait(); err != nil {
			return err
		}
		c.ports[subTopic] = bp
	}
	dev.AttachTo(bp, 0)
	return nil
}

// Detach detaches a device from a topic
func (c *Connector) Detach(subTopic string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.ports == nil {
		return nil
	}
	bp := c.ports[subTopic]
	if bp != nil {
		delete(c.ports, subTopic)
		bp.unsubscribe()
		bp.device.AttachTo(nil, 0)
	}
	return nil
}

func (c *Connector) onConnect(client mqtt.Client) {
	c.Connected()
	if c.onConnectHandler != nil {
		c.onConnectHandler(client)
	}
}

func (c *Connector) subscribeCtl(subTopic string, handler mqtt.MessageHandler) mqtt.Token {
	topic := c.TopicBase + "/" + subTopic + "/ctl"
	return c.Client.Subscribe(topic, 0, handler)
}

func (c *Connector) unsubscribeCtl(subTopic string) mqtt.Token {
	topic := c.TopicBase + "/" + subTopic + "/ctl"
	return c.Client.Unsubscribe(topic)
}

func (c *Connector) publishMsg(subTopic string, msg *tbus.Msg) mqtt.Token {
	var buf bytes.Buffer
	msg.EncodeTo(&buf)
	return c.Client.Publish(c.TopicBase+"/"+subTopic, 0, false, buf.Bytes())
}

func (c *Connector) publishCtlMsg(subTopic string, msg *tbus.Msg) mqtt.Token {
	var buf bytes.Buffer
	msg.EncodeTo(&buf)
	return c.Client.Publish(c.TopicBase+"/"+subTopic+"/ctl", 0, false, buf.Bytes())
}
