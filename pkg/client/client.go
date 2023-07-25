/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"errors"
	"fmt"
	"bytes"
	"net/http"
	"strings"
	"strconv"
	"gopkg.in/yaml.v3"
	"time"
	clientv3 "go.etcd.io/etcd/client/v3"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Options are creation options for a Client.
type Options struct {
	// HTTPClient is the HTTP client to use for requests.
	HTTPClient *http.Client

	// Scheme, if provided, will be used to map go structs to GroupVersionKinds
	Scheme *runtime.Scheme

	// Mapper, if provided, will be used to map GroupVersionKinds to Resources
	Mapper meta.RESTMapper

	// Cache, if provided, is used to read objects from the cache.
	Cache *CacheOptions

	// WarningHandler is used to configure the warning handler responsible for
	// surfacing and handling warnings messages sent by the API server.
	WarningHandler WarningHandlerOptions

	// DryRun instructs the client to only perform dry run requests.
	DryRun *bool
}

// WarningHandlerOptions are options for configuring a
// warning handler for the client which is responsible
// for surfacing API Server warnings.
type WarningHandlerOptions struct {
	// SuppressWarnings decides if the warnings from the
	// API server are suppressed or surfaced in the client.
	SuppressWarnings bool
	// AllowDuplicateLogs does not deduplicate the to-be
	// logged surfaced warnings messages. See
	// log.WarningHandlerOptions for considerations
	// regarding deduplication
	AllowDuplicateLogs bool
}

// CacheOptions are options for creating a cache-backed client.
type CacheOptions struct {
	// Reader is a cache-backed reader that will be used to read objects from the cache.
	// +required
	Reader Reader
	// DisableFor is a list of objects that should not be read from the cache.
	DisableFor []Object
	// Unstructured is a flag that indicates whether the cache-backed client should
	// read unstructured objects or lists from the cache.
	Unstructured bool
}

// NewClientFunc allows a user to define how to create a client.
type NewClientFunc func(config *rest.Config, options Options) (Client, error)

// New returns a new Client using the provided config and Options.
// The returned client reads *and* writes directly from the server
// (it doesn't use object caches).  It understands how to work with
// normal types (both custom resources and aggregated/built-in resources),
// as well as unstructured types.
//
// In the case of normal types, the scheme will be used to look up the
// corresponding group, version, and kind for the given type.  In the
// case of unstructured types, the group, version, and kind will be extracted
// from the corresponding fields on the object.
func New(config *rest.Config, options Options) (c Client, err error) {
	c, err = newClient(config, options)
	if err == nil && options.DryRun != nil && *options.DryRun {
		c = NewDryRunClient(c)
	}
	return c, err
}

func newClient(config *rest.Config, options Options) (*client, error) {
	if config == nil {
		return nil, fmt.Errorf("must provide non-nil rest.Config to client.New")
	}

	if !options.WarningHandler.SuppressWarnings {
		// surface warnings
		logger := log.Log.WithName("KubeAPIWarningLogger")
		// Set a WarningHandler, the default WarningHandler
		// is log.KubeAPIWarningLogger with deduplication enabled.
		// See log.KubeAPIWarningLoggerOptions for considerations
		// regarding deduplication.
		config = rest.CopyConfig(config)
		config.WarningHandler = log.NewKubeAPIWarningLogger(
			logger,
			log.KubeAPIWarningLoggerOptions{
				Deduplicate: !options.WarningHandler.AllowDuplicateLogs,
			},
		)
	}

	// Use the rest HTTP client for the provided config if unset
	if options.HTTPClient == nil {
		var err error
		options.HTTPClient, err = rest.HTTPClientFor(config)
		if err != nil {
			return nil, err
		}
	}

	// Init a scheme if none provided
	if options.Scheme == nil {
		options.Scheme = scheme.Scheme
	}

	// Init a Mapper if none provided
	if options.Mapper == nil {
		var err error
		options.Mapper, err = apiutil.NewDynamicRESTMapper(config, options.HTTPClient)
		if err != nil {
			return nil, err
		}
	}

	resources := &clientRestResources{
		httpClient: options.HTTPClient,
		config:     config,
		scheme:     options.Scheme,
		mapper:     options.Mapper,
		codecs:     serializer.NewCodecFactory(options.Scheme),

		structuredResourceByType:   make(map[schema.GroupVersionKind]*resourceMeta),
		unstructuredResourceByType: make(map[schema.GroupVersionKind]*resourceMeta),
	}

	rawMetaClient, err := metadata.NewForConfigAndClient(config, options.HTTPClient)
	if err != nil {
		return nil, fmt.Errorf("unable to construct metadata-only client for use as part of client: %w", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.224.4.175:2379", "10.224.4.176:22379", "10.224.4.177:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd client failed.")
	}

	c := &client{
		typedClient: typedClient{
			resources:  resources,
			paramCodec: runtime.NewParameterCodec(options.Scheme),
		},
		unstructuredClient: unstructuredClient{
			resources:  resources,
			paramCodec: noConversionParamCodec{},
		},
		metadataClient: metadataClient{
			client:     rawMetaClient,
			restMapper: options.Mapper,
		},
		scheme: options.Scheme,
		mapper: options.Mapper,
		etcdClient: etcdClient,
	}
	if options.Cache == nil || options.Cache.Reader == nil {
		return c, nil
	}

	// We want a cache if we're here.
	// Set the cache.
	c.cache = options.Cache.Reader

	// Load uncached GVKs.
	c.cacheUnstructured = options.Cache.Unstructured
	c.uncachedGVKs = map[schema.GroupVersionKind]struct{}{}
	for _, obj := range options.Cache.DisableFor {
		gvk, err := c.GroupVersionKindFor(obj)
		if err != nil {
			return nil, err
		}
		c.uncachedGVKs[gvk] = struct{}{}
	}
	return c, nil
}

var _ Client = &client{}

type ReconciledObject struct {
	Object runtime.Object
	Action string
}

type TxnBuffer struct {
	ReconciledObjects []ReconciledObject
}

// client is a client.Client that reads and writes directly from/to an API server.
// It lazily initializes new clients at the time they are used.
type client struct {
	typedClient        typedClient
	unstructuredClient unstructuredClient
	metadataClient     metadataClient
	scheme             *runtime.Scheme
	mapper             meta.RESTMapper

	cache             Reader
	uncachedGVKs      map[schema.GroupVersionKind]struct{}
	cacheUnstructured bool

	txnBuffer		  TxnBuffer
	etcdClient		  *clientv3.Client
}

func (c *client) shouldBypassCache(obj runtime.Object) (bool, error) {
	if c.cache == nil {
		return true, nil
	}

	gvk, err := c.GroupVersionKindFor(obj)
	if err != nil {
		return false, err
	}
	// TODO: this is producing unsafe guesses that don't actually work,
	// but it matches ~99% of the cases out there.
	if meta.IsListType(obj) {
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	}
	if _, isUncached := c.uncachedGVKs[gvk]; isUncached {
		return true, nil
	}
	if !c.cacheUnstructured {
		_, isUnstructured := obj.(runtime.Unstructured)
		return isUnstructured, nil
	}
	return false, nil
}

// resetGroupVersionKind is a helper function to restore and preserve GroupVersionKind on an object.
func (c *client) resetGroupVersionKind(obj runtime.Object, gvk schema.GroupVersionKind) {
	if gvk != schema.EmptyObjectKind.GroupVersionKind() {
		if v, ok := obj.(schema.ObjectKind); ok {
			v.SetGroupVersionKind(gvk)
		}
	}
}

// GroupVersionKindFor returns the GroupVersionKind for the given object.
func (c *client) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return apiutil.GVKForObject(obj, c.scheme)
}

// IsObjectNamespaced returns true if the GroupVersionKind of the object is namespaced.
func (c *client) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return apiutil.IsObjectNamespaced(obj, c.scheme, c.mapper)
}

// Scheme returns the scheme this client is using.
func (c *client) Scheme() *runtime.Scheme {
	return c.scheme
}

// RESTMapper returns the scheme this client is using.
func (c *client) RESTMapper() meta.RESTMapper {
	return c.mapper
}

const TESTING = false

func (c *client) TransformObjectToETCDValue(obj runtime.Object) (string, error) {
	const mediaType = runtime.ContentTypeYAML
	info, ok := runtime.SerializerInfoForMediaType(c.typedClient.resources.codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return "", fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}

	objectGroup := obj.GetObjectKind().GroupVersionKind().Group
	objectVersion := obj.GetObjectKind().GroupVersionKind().Version
	groupVersion := schema.GroupVersion{Group: objectGroup, Version: objectVersion}

	encoder := c.typedClient.resources.codecs.EncoderForVersion(info.Serializer, groupVersion)
	var buf bytes.Buffer
	err := encoder.Encode(obj, &buf)
	if err != nil {
		return "", fmt.Errorf("failed to encode object: %v", err)
	}

	// Serialize the etcd value.
	serializedValue, err := yaml.Marshal(buf.String())
	if err != nil {
		return "", fmt.Errorf("failed to serialize etcd value: %v", err)
	}

	slices := strings.Split(string(serializedValue), "\n")
	objectYaml := ""
	for i := 1; i <len(slices); i++ {
		if len(slices[i]) >= 4 {
			objectYaml += slices[i][4:] + "\n"
		}
	}

	return objectYaml, nil
}

// Commit implements client.Client.
func (c *client) Commit(ctx context.Context) error {
	fmt.Printf("\nNUMBER OF RECONCILED OBJECTS: %d\n\n", len(c.txnBuffer.ReconciledObjects))

	compares := make(map[string]int64)
	requests := make(map[string]string)

	for i := 0; i < len(c.txnBuffer.ReconciledObjects); i++ {
		reconciledObject := c.txnBuffer.ReconciledObjects[i]
		key, resourceVersion := GetKeyResourceVersion(reconciledObject.Object)
		switch reconciledObject.Action {
		case "CREATE":
			compares[key] = int64(0)
			value, err := c.TransformObjectToETCDValue(reconciledObject.Object)
			if err != nil {
				return err
			}
			requests[key] = value
		case "DELETE":
			compares[key] = resourceVersion
			requests[key] = ""
		case "GET":
			compares[key] = resourceVersion
		// UPDATE, PATCH
		default:
			compares[key] = resourceVersion
			value, err := c.TransformObjectToETCDValue(reconciledObject.Object)
			if err != nil {
				return err
			}
			requests[key] = value
		}
	}

	etcdCompares := []clientv3.Cmp{}
	for key, rv := range compares {
		etcdCompares = append(etcdCompares, clientv3.Compare(clientv3.ModRevision(key), "=", rv))
	}

	etcdRequests := []clientv3.Op{}
	for key, value := range requests {
		if value == "" {
			etcdRequests = append(etcdRequests, clientv3.OpDelete(key))	
		} else {
			etcdRequests = append(etcdRequests, clientv3.OpPut(key, value))
		}
	}

	txnResponse, _ := c.etcdClient.KV.Txn(ctx).If(
		etcdCompares...,
	).Then(
		etcdRequests...,
	).Else(
	).Commit()
	
	if txnResponse.Succeeded {
		fmt.Println("Transaction succeeded. Key was either created or updated.")
	} else {
		fmt.Println("Transaction failed. No updates were made.")
	}

	c.txnBuffer.ReconciledObjects = []ReconciledObject{}
	return nil

	// txnRequest := &TxnRequest{
	// 	Compare: compares,
	// 	Request: requests,
	// }

	// payload, err := json.Marshal(txnRequest)
	// if err != nil {
	// 	fmt.Println("Error marshaling JSON:", err)
	// 	return err
	// }

	// Create a new HTTP request with POST method and payload
	// url := "https://10.224.4.161:6443/apis/abc.abc.io/v1/namespaces/default/abcs/txn"
	// httpRequest, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	// if err != nil {
	// 	fmt.Println("Error creating request:", err)
	// 	return err
	// }

	// httpRequest.Header.Set("Content-Type", "application/json")

	// // Send the request to the etcd server
	// resp, err := c.typedClient.resources.httpClient.Do(httpRequest)
	// if err != nil {
	// 	fmt.Println("Error sending request:", err)
	// 	return err
	// }
	// defer resp.Body.Close()

	// fmt.Println("Request:", txnRequest)
	// fmt.Println("Response:", resp)

	/*
	// Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return
	}

	// Print the response status code and body
	fmt.Println("Response status code:", resp.StatusCode)
	fmt.Println("Response body:", string(body))
	*/

	// return nil
}

func GetKeyResourceVersion(obj runtime.Object) (string, int64) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", 0
	}
	version := accessor.GetResourceVersion()
	if len(version) == 0 {
		return "", 0
	}
	resourceVersion, _ := strconv.ParseUint(version, 10, 64)
	key := GetObjectKey(obj)
	
	return key, int64(resourceVersion)
}

func GetObjectKey(obj runtime.Object) string {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	objectGroup := strings.ToLower(obj.GetObjectKind().GroupVersionKind().Group)
	objectKind := strings.ToLower(obj.GetObjectKind().GroupVersionKind().Kind)

	key := "/registry/"

	// TODO: implement for every core resource
	if objectGroup == "" || objectGroup == "apps" || objectGroup == "policy" {
		key += objectKind + "s/"
	} else {
		key += objectGroup + "/" + objectKind + "s/"
	}
	if objectKind == "service" {
		key += "specs/"
	}
	key += accessor.GetNamespace() + "/" + accessor.GetName()

	return key
}

// Create implements client.Client.
func (c *client) Create(ctx context.Context, obj Object, opts ...CreateOption) error {
	c.txnBuffer.ReconciledObjects = append(c.txnBuffer.ReconciledObjects, ReconciledObject{Object: obj, Action: "CREATE"})
	if TESTING {
		return nil
	}

	switch obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.Create(ctx, obj, opts...)
	case *metav1.PartialObjectMetadata:
		return fmt.Errorf("cannot create using only metadata")
	default:
		return c.typedClient.Create(ctx, obj, opts...)
	}
}

// Update implements client.Client.
func (c *client) Update(ctx context.Context, obj Object, opts ...UpdateOption) error {
	c.txnBuffer.ReconciledObjects = append(c.txnBuffer.ReconciledObjects, ReconciledObject{Object: obj, Action: "UPDATE"})
	if TESTING {
		return nil
	}

	defer c.resetGroupVersionKind(obj, obj.GetObjectKind().GroupVersionKind())
	switch obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.Update(ctx, obj, opts...)
	case *metav1.PartialObjectMetadata:
		return fmt.Errorf("cannot update using only metadata -- did you mean to patch?")
	default:
		return c.typedClient.Update(ctx, obj, opts...)
	}
}

// Delete implements client.Client.
func (c *client) Delete(ctx context.Context, obj Object, opts ...DeleteOption) error {
	c.txnBuffer.ReconciledObjects = append(c.txnBuffer.ReconciledObjects, ReconciledObject{Object: obj, Action: "DELETE"})
	if TESTING {
		return nil
	}

	switch obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.Delete(ctx, obj, opts...)
	case *metav1.PartialObjectMetadata:
		return c.metadataClient.Delete(ctx, obj, opts...)
	default:
		return c.typedClient.Delete(ctx, obj, opts...)
	}
}

// DeleteAllOf implements client.Client.
func (c *client) DeleteAllOf(ctx context.Context, obj Object, opts ...DeleteAllOfOption) error {
	switch obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.DeleteAllOf(ctx, obj, opts...)
	case *metav1.PartialObjectMetadata:
		return c.metadataClient.DeleteAllOf(ctx, obj, opts...)
	default:
		return c.typedClient.DeleteAllOf(ctx, obj, opts...)
	}
}

// Patch implements client.Client.
func (c *client) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	c.txnBuffer.ReconciledObjects = append(c.txnBuffer.ReconciledObjects, ReconciledObject{Object: obj, Action: "PATCH"})
	if TESTING {
		return nil
	}

	defer c.resetGroupVersionKind(obj, obj.GetObjectKind().GroupVersionKind())
	switch obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.Patch(ctx, obj, patch, opts...)
	case *metav1.PartialObjectMetadata:
		return c.metadataClient.Patch(ctx, obj, patch, opts...)
	default:
		return c.typedClient.Patch(ctx, obj, patch, opts...)
	}
}

// Get implements client.Client.
func (c *client) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	c.txnBuffer.ReconciledObjects = append(c.txnBuffer.ReconciledObjects, ReconciledObject{Object: obj, Action: "GET"})

	if isUncached, err := c.shouldBypassCache(obj); err != nil {
		return err
	} else if !isUncached {
		return c.cache.Get(ctx, key, obj, opts...)
	}

	switch obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.Get(ctx, key, obj, opts...)
	case *metav1.PartialObjectMetadata:
		// Metadata only object should always preserve the GVK coming in from the caller.
		defer c.resetGroupVersionKind(obj, obj.GetObjectKind().GroupVersionKind())
		return c.metadataClient.Get(ctx, key, obj, opts...)
	default:
		return c.typedClient.Get(ctx, key, obj, opts...)
	}
}

// List implements client.Client.
func (c *client) List(ctx context.Context, obj ObjectList, opts ...ListOption) error {
	if isUncached, err := c.shouldBypassCache(obj); err != nil {
		return err
	} else if !isUncached {
		return c.cache.List(ctx, obj, opts...)
	}

	switch x := obj.(type) {
	case runtime.Unstructured:
		return c.unstructuredClient.List(ctx, obj, opts...)
	case *metav1.PartialObjectMetadataList:
		// Metadata only object should always preserve the GVK.
		gvk := obj.GetObjectKind().GroupVersionKind()
		defer c.resetGroupVersionKind(obj, gvk)

		// Call the list client.
		if err := c.metadataClient.List(ctx, obj, opts...); err != nil {
			return err
		}

		// Restore the GVK for each item in the list.
		itemGVK := schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			// TODO: this is producing unsafe guesses that don't actually work,
			// but it matches ~99% of the cases out there.
			Kind: strings.TrimSuffix(gvk.Kind, "List"),
		}
		for i := range x.Items {
			item := &x.Items[i]
			item.SetGroupVersionKind(itemGVK)
		}

		return nil
	default:
		return c.typedClient.List(ctx, obj, opts...)
	}
}

// Status implements client.StatusClient.
func (c *client) Status() SubResourceWriter {
	return c.SubResource("status")
}

func (c *client) SubResource(subResource string) SubResourceClient {
	return &subResourceClient{client: c, subResource: subResource}
}

// subResourceClient is client.SubResourceWriter that writes to subresources.
type subResourceClient struct {
	client      *client
	subResource string
}

// ensure subResourceClient implements client.SubResourceClient.
var _ SubResourceClient = &subResourceClient{}

// SubResourceGetOptions holds all the possible configuration
// for a subresource Get request.
type SubResourceGetOptions struct {
	Raw *metav1.GetOptions
}

// ApplyToSubResourceGet updates the configuaration to the given get options.
func (getOpt *SubResourceGetOptions) ApplyToSubResourceGet(o *SubResourceGetOptions) {
	if getOpt.Raw != nil {
		o.Raw = getOpt.Raw
	}
}

// ApplyOptions applues the given options.
func (getOpt *SubResourceGetOptions) ApplyOptions(opts []SubResourceGetOption) *SubResourceGetOptions {
	for _, o := range opts {
		o.ApplyToSubResourceGet(getOpt)
	}

	return getOpt
}

// AsGetOptions returns the configured options as *metav1.GetOptions.
func (getOpt *SubResourceGetOptions) AsGetOptions() *metav1.GetOptions {
	if getOpt.Raw == nil {
		return &metav1.GetOptions{}
	}
	return getOpt.Raw
}

// SubResourceUpdateOptions holds all the possible configuration
// for a subresource update request.
type SubResourceUpdateOptions struct {
	UpdateOptions
	SubResourceBody Object
}

// ApplyToSubResourceUpdate updates the configuration on the given create options
func (uo *SubResourceUpdateOptions) ApplyToSubResourceUpdate(o *SubResourceUpdateOptions) {
	uo.UpdateOptions.ApplyToUpdate(&o.UpdateOptions)
	if uo.SubResourceBody != nil {
		o.SubResourceBody = uo.SubResourceBody
	}
}

// ApplyOptions applies the given options.
func (uo *SubResourceUpdateOptions) ApplyOptions(opts []SubResourceUpdateOption) *SubResourceUpdateOptions {
	for _, o := range opts {
		o.ApplyToSubResourceUpdate(uo)
	}

	return uo
}

// SubResourceUpdateAndPatchOption is an option that can be used for either
// a subresource update or patch request.
type SubResourceUpdateAndPatchOption interface {
	SubResourceUpdateOption
	SubResourcePatchOption
}

// WithSubResourceBody returns an option that uses the given body
// for a subresource Update or Patch operation.
func WithSubResourceBody(body Object) SubResourceUpdateAndPatchOption {
	return &withSubresourceBody{body: body}
}

type withSubresourceBody struct {
	body Object
}

func (wsr *withSubresourceBody) ApplyToSubResourceUpdate(o *SubResourceUpdateOptions) {
	o.SubResourceBody = wsr.body
}

func (wsr *withSubresourceBody) ApplyToSubResourcePatch(o *SubResourcePatchOptions) {
	o.SubResourceBody = wsr.body
}

// SubResourceCreateOptions are all the possible configurations for a subresource
// create request.
type SubResourceCreateOptions struct {
	CreateOptions
}

// ApplyOptions applies the given options.
func (co *SubResourceCreateOptions) ApplyOptions(opts []SubResourceCreateOption) *SubResourceCreateOptions {
	for _, o := range opts {
		o.ApplyToSubResourceCreate(co)
	}

	return co
}

// ApplyToSubresourceCreate applies the the configuration on the given create options.
func (co *SubResourceCreateOptions) ApplyToSubresourceCreate(o *SubResourceCreateOptions) {
	co.CreateOptions.ApplyToCreate(&co.CreateOptions)
}

// SubResourcePatchOptions holds all possible configurations for a subresource patch
// request.
type SubResourcePatchOptions struct {
	PatchOptions
	SubResourceBody Object
}

// ApplyOptions applies the given options.
func (po *SubResourcePatchOptions) ApplyOptions(opts []SubResourcePatchOption) *SubResourcePatchOptions {
	for _, o := range opts {
		o.ApplyToSubResourcePatch(po)
	}

	return po
}

// ApplyToSubResourcePatch applies the configuration on the given patch options.
func (po *SubResourcePatchOptions) ApplyToSubResourcePatch(o *SubResourcePatchOptions) {
	po.PatchOptions.ApplyToPatch(&o.PatchOptions)
	if po.SubResourceBody != nil {
		o.SubResourceBody = po.SubResourceBody
	}
}

func (sc *subResourceClient) Get(ctx context.Context, obj Object, subResource Object, opts ...SubResourceGetOption) error {
	switch obj.(type) {
	case runtime.Unstructured:
		return sc.client.unstructuredClient.GetSubResource(ctx, obj, subResource, sc.subResource, opts...)
	case *metav1.PartialObjectMetadata:
		return errors.New("can not get subresource using only metadata")
	default:
		return sc.client.typedClient.GetSubResource(ctx, obj, subResource, sc.subResource, opts...)
	}
}

// Create implements client.SubResourceClient
func (sc *subResourceClient) Create(ctx context.Context, obj Object, subResource Object, opts ...SubResourceCreateOption) error {
	defer sc.client.resetGroupVersionKind(obj, obj.GetObjectKind().GroupVersionKind())
	defer sc.client.resetGroupVersionKind(subResource, subResource.GetObjectKind().GroupVersionKind())

	switch obj.(type) {
	case runtime.Unstructured:
		return sc.client.unstructuredClient.CreateSubResource(ctx, obj, subResource, sc.subResource, opts...)
	case *metav1.PartialObjectMetadata:
		return fmt.Errorf("cannot update status using only metadata -- did you mean to patch?")
	default:
		return sc.client.typedClient.CreateSubResource(ctx, obj, subResource, sc.subResource, opts...)
	}
}

// Update implements client.SubResourceClient
func (sc *subResourceClient) Update(ctx context.Context, obj Object, opts ...SubResourceUpdateOption) error {
	defer sc.client.resetGroupVersionKind(obj, obj.GetObjectKind().GroupVersionKind())
	switch obj.(type) {
	case runtime.Unstructured:
		return sc.client.unstructuredClient.UpdateSubResource(ctx, obj, sc.subResource, opts...)
	case *metav1.PartialObjectMetadata:
		return fmt.Errorf("cannot update status using only metadata -- did you mean to patch?")
	default:
		return sc.client.typedClient.UpdateSubResource(ctx, obj, sc.subResource, opts...)
	}
}

// Patch implements client.SubResourceWriter.
func (sc *subResourceClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...SubResourcePatchOption) error {
	defer sc.client.resetGroupVersionKind(obj, obj.GetObjectKind().GroupVersionKind())
	switch obj.(type) {
	case runtime.Unstructured:
		return sc.client.unstructuredClient.PatchSubResource(ctx, obj, sc.subResource, patch, opts...)
	case *metav1.PartialObjectMetadata:
		return sc.client.metadataClient.PatchSubResource(ctx, obj, sc.subResource, patch, opts...)
	default:
		return sc.client.typedClient.PatchSubResource(ctx, obj, sc.subResource, patch, opts...)
	}
}
