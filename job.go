package main

import (
    "fmt"
    "github.com/mongodb/mongo-go-driver/bson"
    "github.com/mongodb/mongo-go-driver/bson/objectid"
)

type job struct {
    id objectid.ObjectID
    status string
    srcUrl string
    dstUrl string
    description string
}

func (j *job) init(document *bson.Document) error {

    var val *bson.Value

    if document == nil {
        return fmt.Errorf("can't init from nil document")
    }

    if val = document.Lookup("_id"); val == nil {
        return fmt.Errorf("field _id missing")
    }
    j.id = val.ObjectID()

    if val = document.Lookup("status"); val == nil {
        return fmt.Errorf("field status missing")
    }
    j.status = val.StringValue()

    if val = document.Lookup("srcUrl"); val == nil {
        return fmt.Errorf("field srcUrl missing")
    }
    j.srcUrl = val.StringValue()

    if val = document.Lookup("dstUrl"); val == nil {
        return fmt.Errorf("field dstUrl missing")
    }
    j.dstUrl = val.StringValue()

    return nil
}
