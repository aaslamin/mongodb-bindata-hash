package main

import (
	"fmt"

	"github.com/globalsign/mgo/bson"
)

type Zonable interface {
	SetZHash(interface{})
	SetZone(int)
	SetID(bson.ObjectId)
}

type ZHashInteger struct {
	ID    bson.ObjectId `bson:"_id"`
	Zone  int           `bson:"zone"`
	ZHash int           `bson:"zhash"`
}

func (z *ZHashInteger) SetZHash(hash interface{}) {
	h, ok := hash.(int)
	if !ok {
		panic(fmt.Errorf("ZHashInteger: expecting an integer zhash, but received type %T", hash))
	}

	z.ZHash = h
}

func (z *ZHashInteger) SetZone(zone int) {
	z.Zone = zone
}

func (z *ZHashInteger) SetID(id bson.ObjectId) {
	z.ID = id
}

type ZHashBinary struct {
	ID    bson.ObjectId `bson:"_id"`
	Zone  int           `bson:"zone"`
	ZHash bson.Binary   `bson:"zhash"`
}

func (z *ZHashBinary) SetZHash(hash interface{}) {
	h, ok := hash.(bson.Binary)
	if !ok {
		panic(fmt.Errorf("ZHashBinary: expecting a binary zhash, but received type %T", hash))
	}

	z.ZHash = h
}

func (z *ZHashBinary) SetZone(zone int) {
	z.Zone = zone
}

func (z *ZHashBinary) SetID(id bson.ObjectId) {
	z.ID = id
}
