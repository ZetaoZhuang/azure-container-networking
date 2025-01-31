package nmagent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"

	"github.com/Azure/azure-container-networking/nmagent/internal"
	"github.com/pkg/errors"
)

// Request represents an abstracted HTTP request, capable of validating itself,
// producing a valid Path, Body, and its Method.
type Request interface {
	// Validate should ensure that the request is valid to submit
	Validate() error

	// Path should produce a URL path, complete with any URL parameters
	// interpolated
	Path() string

	// Body produces the HTTP request body necessary to submit the request
	Body() (io.Reader, error)

	// Method returns the HTTP Method to be used for the request.
	Method() string
}

var _ Request = &PutNetworkContainerRequest{}

// PutNetworkContainerRequest is a collection of parameters necessary to create
// a new network container
type PutNetworkContainerRequest struct {
	ID     string `json:"networkContainerID"` // the id of the network container
	VNetID string `json:"virtualNetworkID"`   // the id of the customer's vnet

	// Version is the new network container version
	Version uint64 `json:"version"`

	// SubnetName is the name of the delegated subnet. This is used to
	// authenticate the request. The list of ipv4addresses must be contained in
	// the subnet's prefix.
	SubnetName string `json:"subnetName"`

	// IPv4 addresses in the customer virtual network that will be assigned to
	// the interface.
	IPv4Addrs []string `json:"ipV4Addresses"`

	Policies []Policy `json:"policies"` // policies applied to the network container

	// VlanID is used to distinguish Network Containers with duplicate customer
	// addresses. "0" is considered a default value by the API.
	VlanID int `json:"vlanId"`

	GREKey uint16 `json:"greKey"`

	// AuthenticationToken is the base64 security token for the subnet containing
	// the Network Container addresses
	AuthenticationToken string `json:"-"`

	// PrimaryAddress is the primary customer address of the interface in the
	// management VNet
	PrimaryAddress string `json:"-"`
}

// Body marshals the JSON fields of the request and produces an Reader intended
// for use with an HTTP request
func (p *PutNetworkContainerRequest) Body() (io.Reader, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling PutNetworkContainerRequest")
	}

	return bytes.NewReader(body), nil
}

// Method returns the HTTP method for this request type
func (p *PutNetworkContainerRequest) Method() string {
	return http.MethodPost
}

// Path returns the URL path necessary to submit this PutNetworkContainerRequest
func (p *PutNetworkContainerRequest) Path() string {
	const PutNCRequestPath string = "/NetworkManagement/interfaces/%s/networkContainers/%s/authenticationToken/%s/api-version/1"
	return fmt.Sprintf(PutNCRequestPath, p.PrimaryAddress, p.ID, p.AuthenticationToken)
}

// Validate ensures that all of the required parameters of the request have
// been filled out properly prior to submission to NMAgent
func (p *PutNetworkContainerRequest) Validate() error {
	err := internal.ValidationError{}

	if p.Version == 0 {
		err.MissingFields = append(err.MissingFields, "Version")
	}

	if p.SubnetName == "" {
		err.MissingFields = append(err.MissingFields, "SubnetName")
	}

	if len(p.IPv4Addrs) == 0 {
		err.MissingFields = append(err.MissingFields, "IPv4Addrs")
	}

	if p.VNetID == "" {
		err.MissingFields = append(err.MissingFields, "VNetID")
	}

	if err.IsEmpty() {
		return nil
	}
	return err
}

type Policy struct {
	ID   string
	Type string
}

// MarshalJson encodes policies as a JSON string, separated by a comma. This
// specific format is requested by the NMAgent documentation
func (p Policy) MarshalJSON() ([]byte, error) {
	out := bytes.NewBufferString(p.ID)
	out.WriteString(", ")
	out.WriteString(p.Type)

	outStr := out.String()
	// nolint:wrapcheck // wrapping this error provides no useful information
	return json.Marshal(outStr)
}

// UnmarshalJSON decodes a JSON-encoded policy string
func (p *Policy) UnmarshalJSON(in []byte) error {
	const expectedNumParts = 2

	var raw string
	err := json.Unmarshal(in, &raw)
	if err != nil {
		return errors.Wrap(err, "decoding policy")
	}

	parts := strings.Split(raw, ",")
	if len(parts) != expectedNumParts {
		return errors.New("policies must be two comma-separated values")
	}

	p.ID = strings.TrimFunc(parts[0], unicode.IsSpace)
	p.Type = strings.TrimFunc(parts[1], unicode.IsSpace)

	return nil
}

var _ Request = JoinNetworkRequest{}

type JoinNetworkRequest struct {
	NetworkID string `validate:"presence" json:"-"` // the customer's VNet ID
}

// Path constructs a URL path for invoking a JoinNetworkRequest using the
// provided parameters
func (j JoinNetworkRequest) Path() string {
	const JoinNetworkPath string = "/NetworkManagement/joinedVirtualNetworks/%s/api-version/1"
	return fmt.Sprintf(JoinNetworkPath, j.NetworkID)
}

// Body returns nothing, because JoinNetworkRequest has no request body
func (j JoinNetworkRequest) Body() (io.Reader, error) {
	return nil, nil
}

// Method returns the HTTP request method to submit a JoinNetworkRequest
func (j JoinNetworkRequest) Method() string {
	return http.MethodPost
}

// Validate ensures that the provided parameters of the request are valid
func (j JoinNetworkRequest) Validate() error {
	err := internal.ValidationError{}

	if j.NetworkID == "" {
		err.MissingFields = append(err.MissingFields, "NetworkID")
	}

	if err.IsEmpty() {
		return nil
	}
	return err
}

var _ Request = DeleteContainerRequest{}

// DeleteContainerRequest represents all information necessary to request that
// NMAgent delete a particular network container
type DeleteContainerRequest struct {
	NCID string `json:"-"` // the Network Container ID

	// PrimaryAddress is the primary customer address of the interface in the
	// management VNET
	PrimaryAddress      string `json:"-"`
	AuthenticationToken string `json:"-"`
}

// Path returns the path for submitting a DeleteContainerRequest with
// parameters interpolated correctly
func (d DeleteContainerRequest) Path() string {
	const DeleteNCPath string = "/NetworkManagement/interfaces/%s/networkContainers/%s/authenticationToken/%s/api-version/1/method/DELETE"
	return fmt.Sprintf(DeleteNCPath, d.PrimaryAddress, d.NCID, d.AuthenticationToken)
}

// Body returns nothing, because DeleteContainerRequests have no HTTP body
func (d DeleteContainerRequest) Body() (io.Reader, error) {
	return nil, nil
}

// Method returns the HTTP method required to submit a DeleteContainerRequest
func (d DeleteContainerRequest) Method() string {
	return http.MethodPost
}

// Validate ensures that the DeleteContainerRequest has the correct information
// to submit the request
func (d DeleteContainerRequest) Validate() error {
	err := internal.ValidationError{}

	if d.NCID == "" {
		err.MissingFields = append(err.MissingFields, "NCID")
	}

	if d.PrimaryAddress == "" {
		err.MissingFields = append(err.MissingFields, "PrimaryAddress")
	}

	if d.AuthenticationToken == "" {
		err.MissingFields = append(err.MissingFields, "AuthenticationToken")
	}

	if err.IsEmpty() {
		return nil
	}
	return err
}

var _ Request = GetNetworkConfigRequest{}

// GetNetworkConfigRequest is a collection of necessary information for
// submitting a request for a customer's network configuration
type GetNetworkConfigRequest struct {
	VNetID string `json:"-"` // the customer's virtual network ID
}

// Path produces a URL path used to submit a request
func (g GetNetworkConfigRequest) Path() string {
	const GetNetworkConfigPath string = "/NetworkManagement/joinedVirtualNetworks/%s/api-version/1"
	return fmt.Sprintf(GetNetworkConfigPath, g.VNetID)
}

// Body returns nothing because GetNetworkConfigRequest has no HTTP request
// body
func (g GetNetworkConfigRequest) Body() (io.Reader, error) {
	return nil, nil
}

// Method returns the HTTP method required to submit a GetNetworkConfigRequest
func (g GetNetworkConfigRequest) Method() string {
	return http.MethodGet
}

// Validate ensures that the request is complete and the parameters are correct
func (g GetNetworkConfigRequest) Validate() error {
	err := internal.ValidationError{}

	if g.VNetID == "" {
		err.MissingFields = append(err.MissingFields, "VNetID")
	}

	if err.IsEmpty() {
		return nil
	}
	return err
}
