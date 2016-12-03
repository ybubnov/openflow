package ofp

import (
	"bytes"
	"fmt"
	"io"

	"github.com/netrack/openflow/internal/encoding"
)

const (
	// Setup the next table in the lookup pipeline.
	InstructionTypeGotoTable InstructionType = 1 + iota

	// Setup the metadata field for use later in pipeline.
	InstructionTypeWriteMetadata InstructionType = 1 + iota

	// Write the action(s) onto the datapath action set.
	InstructionTypeWriteActions InstructionType = 1 + iota

	// Applies the action(s) immediately.
	InstructionTypeApplyActions InstructionType = 1 + iota

	// Clears all actions from the datapath action set.
	InstructionTypeClearActions InstructionType = 1 + iota

	// Apply meter (rate limiter).
	InstructionTypeMeter InstructionType = 1 + iota

	// Experimenter instruction.
	InstructionTypeExperimenter InstructionType = 0xffff
)

// InstructionType represents a type of the flow modification instruction.
type InstructionType uint16

var instructionMap = map[InstructionType]encoding.ReaderMaker{
	InstructionTypeGotoTable:     encoding.ReaderMakerOf(InstructionGotoTable{}),
	InstructionTypeWriteMetadata: encoding.ReaderMakerOf(InstructionWriteMetadata{}),
	InstructionTypeApplyActions:  encoding.ReaderMakerOf(InstructionApplyActions{}),
	InstructionTypeWriteActions:  encoding.ReaderMakerOf(InstructionWriteActions{}),
	InstructionTypeClearActions:  encoding.ReaderMakerOf(InstructionClearActions{}),
	InstructionTypeMeter:         encoding.ReaderMakerOf(InstructionMeter{}),
}

// Instruction header that is common to all instructions. The length
// includes the header and any padding used to make the instruction
// 64-bit aligned.
//
// NB: The length of an instruction *must* always be a multiple of eight.
type instructionhdr struct {
	// Type is an instruction type.
	Type InstructionType

	// Length of this structure in bytes.
	Len uint16
}

const instructionLen uint16 = 8

type Instruction interface {
	encoding.ReadWriter

	// Type returns the type of the instruction.
	Type() InstructionType
}

type Instructions []Instruction

// WriteTo Implements io.WriterTo interface.
func (i *Instructions) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer

	for _, inst := range *i {
		_, err = inst.WriteTo(&buf)
		if err != nil {
			return
		}
	}

	return encoding.WriteTo(w, buf.Bytes())
}

func (i *Instructions) ReadFrom(r io.Reader) (n int64, err error) {
	var instType InstructionType

	rm := func() (io.ReaderFrom, error) {
		if rm, ok := instructionMap[instType]; ok {
			rd, err := rm.MakeReader()
			*i = append(*i, rd.(Instruction))
			return rd, err
		}

		format := "ofp: unknown instruction type: '%x'"
		return nil, fmt.Errorf(format, instType)
	}

	return encoding.ScanFrom(r, &instType,
		encoding.ReaderMakerFunc(rm))
}

// InstructionGotoTable represents a packet processing pipeline
// redirection message.
type InstructionGotoTable struct {
	// Table indicates the next table in the packet processing
	// pipeline.
	Table Table
}

// Type implements Instruction interface and returns the type on
// the instruction.
func (i *InstructionGotoTable) Type() InstructionType {
	return InstructionTypeGotoTable
}

// WriteTo implements WriterTo interface.
func (i *InstructionGotoTable) WriteTo(w io.Writer) (int64, error) {
	return encoding.WriteTo(w, instructionhdr{i.Type(), 8}, i.Table, pad3{})
}

func (i *InstructionGotoTable) ReadFrom(r io.Reader) (int64, error) {
	return encoding.ReadFrom(r, &instructionhdr{}, &i.Table, &defaultPad3)
}

// InstructionWriteMetadata setups metadata fields to use later in
// pipeline.
//
// Metadata for the next table lookup can be written using the Metadata and
// the MetadataMask in order to set specific bits on the match field.
//
// If this instruction is not specified, the metadata is passed, unchanged.
type InstructionWriteMetadata struct {
	// Metadata stores a value to write.
	Metadata uint64

	// MetadataMask specifies a metadata bit mask.
	MetadataMask uint64
}

// Type implements Instruction interface and returns the type of the
// instruction.
func (i *InstructionWriteMetadata) Type() InstructionType {
	return InstructionTypeWriteMetadata
}

// WriteTo implements WriterTo interface.
func (i *InstructionWriteMetadata) WriteTo(w io.Writer) (int64, error) {
	return encoding.WriteTo(w, instructionhdr{i.Type(), 24},
		pad4{}, i.Metadata, i.MetadataMask)
}

func (i *InstructionWriteMetadata) ReadFrom(r io.Reader) (int64, error) {
	return encoding.ReadFrom(r, &instructionhdr{},
		&defaultPad4, &i.Metadata, &i.MetadataMask)
}

func writeInstructionActions(w io.Writer, t InstructionType,
	actions Actions) (int64, error) {

	// Covert the list of actions into the slice of bytes,
	// so we could include the length of the actions into
	// the instruction header.
	buf, err := actions.bytes()
	if err != nil {
		return int64(len(buf)), err
	}

	// Write the header of the instruction with the length,
	// that includes the list of instruction actions.
	header := instructionhdr{t, uint16(len(buf)) + instructionLen}
	return encoding.WriteTo(w, header, pad4{}, buf)
}

func readInstructionActions(r io.Reader, actions Actions) (int64, error) {
	var read int64

	// Read the header of the instruction at first to retrieve
	// the size of actions in the packet.
	var header instructionhdr
	num, err := encoding.ReadFrom(r, &header)
	read += num

	if err != nil {
		return read, err
	}

	// Limit the reader to the size of actions, so we could know
	// where is the a border of the message.
	limrd := io.LimitReader(r, int64(header.Len-4))
	num, err = actions.ReadFrom(limrd)
	read += num

	return read, err
}

// InstructionActions represents a bundle of action instructions.
//
// For the Apply-Actions instruction, the actions field is treated as a
// list and the actions are applied to the packet in-order.
type InstructionApplyActions struct {
	// Actions associated with IT_WRITE_ACTIONS and IT_APPLY_ACTIONS.
	Actions Actions
}

func (i *InstructionApplyActions) Type() InstructionType {
	return InstructionTypeApplyActions
}

// WriteTo implements WriterTo interface.
func (i *InstructionApplyActions) WriteTo(w io.Writer) (int64, error) {
	return writeInstructionActions(w, i.Type(), i.Actions)
}

func (i *InstructionApplyActions) ReadFrom(r io.Reader) (int64, error) {
	return readInstructionActions(r, i.Actions)
}

// For the Write-Actions instruction, the actions field is treated as a set
// and the actions are merged into the current action set.
type InstructionWriteActions struct {
	Actions Actions
}

func (i *InstructionWriteActions) Type() InstructionType {
	return InstructionTypeWriteActions
}

func (i *InstructionWriteActions) WriteTo(w io.Writer) (int64, error) {
	return writeInstructionActions(w, i.Type(), i.Actions)
}

func (i *InstructionWriteActions) ReadFrom(r io.Reader) (int64, error) {
	return readInstructionActions(r, i.Actions)
}

// For the Clear-Actions instruction, the structure does not contain any
// actions.
type InstructionClearActions struct{}

func (i *InstructionClearActions) Type() InstructionType {
	return InstructionTypeClearActions
}

func (i *InstructionClearActions) WriteTo(w io.Writer) (int64, error) {
	return writeInstructionActions(w, i.Type(), nil)
}

func (i *InstructionClearActions) ReadFrom(r io.Reader) (int64, error) {
	return encoding.ReadFrom(r, &defaultPad8)
}

// Instruction structure for IT_METER
type InstructionMeter struct {
	// MeterID indicates which meter to apply on the packet.
	Meter Meter
}

// Type implements Instruction interface and returns type of the
// instruction.
func (i *InstructionMeter) Type() InstructionType {
	return InstructionTypeMeter
}

// WriteTo implements WriterTo interface.
func (i *InstructionMeter) WriteTo(w io.Writer) (int64, error) {
	return encoding.WriteTo(w, instructionhdr{i.Type(), 8}, i.Meter)
}

func (i *InstructionMeter) ReadFrom(r io.Reader) (int64, error) {
	return encoding.ReadFrom(r, &instructionhdr{}, &i.Meter)
}