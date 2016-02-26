/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   nfnty
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

const (
	DivNanoSecond  = 1e0
	DivMicroSecond = 1e3
	DivMilliSecond = 1e6
	DivSecond      = 1e9
	DivMinute      = DivSecond * 60
	DivHour        = DivHour * 60
)

type InfluxdbEncoder struct {
	timestamp_division int64
}

type InfluxdbEncoderConfig struct {
	TimestampPrecision string `toml:"timestamp_precision"`
}

func (encoder *InfluxdbEncoder) ConfigStruct() interface{} {
	return &InfluxdbEncoderConfig{
		TimestampPrecision: "ns",
	}
}

func (encoder *InfluxdbEncoder) Init(config interface{}) (err error) {
	conf := config.(*InfluxdbEncoderConfig)
	switch conf.TimestampPrecision {
	case "ns":
		encoder.timestamp_division = DivNanoSecond
	case "us":
		encoder.timestamp_division = DivMicroSecond
	case "ms":
		encoder.timestamp_division = DivMilliSecond
	case "s":
		encoder.timestamp_division = DivSecond
	case "m":
		encoder.timestamp_division = DivMinute
	case "h":
		encoder.timestamp_division = DivHour
	default:
		return errors.New("timestamp_precision has to be one of [ns, us, ms, s, m, h]")
	}
	return
}

func writeEscMeasure(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == ',' || r == ' ' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func writeEscKey(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == ',' || r == ' ' || r == '=' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func writeEscString(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == '"' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func (encoder *InfluxdbEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	msg := pack.Message
	buf := bytes.Buffer{}

	writeEscMeasure(&buf, *msg.Type)
	buf.WriteString(",Logger=")
	writeEscKey(&buf, *msg.Logger)
	buf.WriteRune(' ')

	first := true
	for _, field := range msg.Fields {
		if first == false {
			buf.WriteRune(',')
		} else {
			first = false
		}
		writeEscKey(&buf, *field.Name)
		buf.WriteRune('=')

		field_type := field.GetValueType()
		switch field_type {
		case message.Field_INTEGER:
			values := field.GetValueInteger()
			if len(values) > 1 {
				err = fmt.Errorf("More than one value: integer: %s", *field.Name)
				return
			}
			buf.WriteString(strconv.FormatInt(values[0], 10))
			buf.WriteRune('i')

		case message.Field_DOUBLE:
			values := field.GetValueDouble()
			if len(values) > 1 {
				err = fmt.Errorf("More than one value: double: %s", *field.Name)
				return
			}
			buf.WriteString(strconv.FormatFloat(values[0], 'f', -1, 64))

		case message.Field_BOOL:
			values := field.GetValueBool()
			if len(values) > 1 {
				err = fmt.Errorf("More than one value: bool: %s", *field.Name)
				return
			}
			buf.WriteString(strconv.FormatBool(values[0]))

		case message.Field_STRING:
			values := field.GetValueString()
			if len(values) > 1 {
				err = fmt.Errorf("More than one value: string: %s", *field.Name)
				return
			}
			buf.WriteRune('"')
			writeEscString(&buf, values[0])
			buf.WriteRune('"')

		default:
			err = fmt.Errorf("Unsupported field type: %s: %s",
				*field.Name, message.Field_ValueType_name[int32(field_type)])
			return
		}
	}

	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(msg.GetTimestamp()/encoder.timestamp_division, 10))
	buf.WriteRune('\n')
	return buf.Bytes(), err
}

func init() {
	pipeline.RegisterPlugin("InfluxdbEncoder", func() interface{} {
		return new(InfluxdbEncoder)
	})
}
