package api

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/davecgh/go-spew/spew"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"

	cclient "github.com/fabriziopandini/cluster-api-provider-goofy/pkg/cloud/runtime/client"

	"github.com/emicklei/go-restful/v3"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/tools/portforward"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cmanager "github.com/fabriziopandini/cluster-api-provider-goofy/pkg/cloud/runtime/manager"
	gportforward "github.com/fabriziopandini/cluster-api-provider-goofy/pkg/server/api/portforward"
)

// ResourceGroupResolver defines a func that can identify which workloadCluster/resourceGroup a
// request targets to.
type ResourceGroupResolver func(host string) (string, error)

// NewAPIServerHandler returns an http.Handler for fake API server.
func NewAPIServerHandler(manager cmanager.Manager, log logr.Logger, resolver ResourceGroupResolver) http.Handler {
	apiServer := &apiServerHandler{
		container:             restful.NewContainer(),
		manager:               manager,
		log:                   log,
		resourceGroupResolver: resolver,
	}

	apiServer.container.Filter(apiServer.globalLogging)

	ws := new(restful.WebService)
	ws.Consumes(runtime.ContentTypeJSON)
	ws.Produces(runtime.ContentTypeJSON)

	// Health check
	ws.Route(ws.GET("/").To(apiServer.healthz))

	// Discovery endpoints
	ws.Route(ws.GET("/api").To(apiServer.apiDiscovery))
	ws.Route(ws.GET("/api/v1").To(apiServer.apiV1Discovery))
	ws.Route(ws.GET("/apis").To(apiServer.apisDiscovery))
	ws.Route(ws.GET("/apis/{group}/{version}").To(apiServer.apisDiscovery))

	// CRUD endpoints (global objects)
	ws.Route(ws.POST("/api/v1/{resource}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Create))
	ws.Route(ws.GET("/api/v1/{resource}").To(apiServer.apiV1List))
	ws.Route(ws.GET("/api/v1/{resource}/{name}").To(apiServer.apiV1Get))
	ws.Route(ws.PUT("/api/v1/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Update))
	ws.Route(ws.PATCH("/api/v1/{resource}/{name}").Consumes(string(types.MergePatchType), string(types.StrategicMergePatchType)).To(apiServer.apiV1Patch))
	ws.Route(ws.DELETE("/api/v1/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Delete))

	ws.Route(ws.POST("/apis/{group}/{version}/{resource}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Create))
	ws.Route(ws.GET("/apis/{group}/{version}/{resource}").To(apiServer.apiV1List))
	ws.Route(ws.GET("/apis/{group}/{version}/{resource}/{name}").To(apiServer.apiV1Get))
	ws.Route(ws.PUT("/apis/{group}/{version}/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Update))
	ws.Route(ws.PATCH("/apis/{group}/{version}/{resource}/{name}").Consumes(string(types.MergePatchType), string(types.StrategicMergePatchType)).To(apiServer.apiV1Patch))
	ws.Route(ws.DELETE("/apis/{group}/{version}/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Delete))

	// CRUD endpoints (namespaced objects)
	ws.Route(ws.POST("/api/v1/namespaces/{namespace}/{resource}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Create))
	ws.Route(ws.GET("/api/v1/namespaces/{namespace}/{resource}").To(apiServer.apiV1List))
	ws.Route(ws.GET("/api/v1/namespaces/{namespace}/{resource}/{name}").To(apiServer.apiV1Get))
	ws.Route(ws.PUT("/api/v1/namespaces/{namespace}/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Update))
	ws.Route(ws.PATCH("/api/v1/namespaces/{namespace}/{resource}/{name}").Consumes(string(types.MergePatchType), string(types.StrategicMergePatchType)).To(apiServer.apiV1Patch))
	ws.Route(ws.DELETE("/api/v1/namespaces/{namespace}/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Delete))

	ws.Route(ws.POST("/apis/{group}/{version}/{resource}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Create))
	ws.Route(ws.GET("/apis/{group}/{version}/namespaces/{namespace}/{resource}").To(apiServer.apiV1List))
	ws.Route(ws.GET("/apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}").To(apiServer.apiV1Get))
	ws.Route(ws.PUT("/apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Update))
	ws.Route(ws.PATCH("/apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}").Consumes(string(types.MergePatchType), string(types.StrategicMergePatchType)).To(apiServer.apiV1Patch))
	ws.Route(ws.DELETE("/apis/{group}/{version}/namespaces/{namespace}/{resource}/{name}").Consumes(runtime.ContentTypeProtobuf).To(apiServer.apiV1Delete))

	// Port forward endpoints
	ws.Route(ws.GET("/api/v1/namespaces/{namespace}/pods/{name}/portforward").To(apiServer.apiV1PortForward))
	ws.Route(ws.POST("/api/v1/namespaces/{namespace}/pods/{name}/portforward").Consumes("*/*").To(apiServer.apiV1PortForward))

	apiServer.container.Add(ws)

	return apiServer
}

type apiServerHandler struct {
	container             *restful.Container
	manager               cmanager.Manager
	log                   logr.Logger
	resourceGroupResolver ResourceGroupResolver
}

func (h *apiServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.container.ServeHTTP(w, r)
}

func (h *apiServerHandler) globalLogging(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	h.log.Info("Serving", "method", req.Request.Method, "url", req.Request.URL, "contentType", req.HeaderParameter("Content-Type"))
	chain.ProcessFilter(req, resp)
}

func (h *apiServerHandler) apiDiscovery(_ *restful.Request, resp *restful.Response) {
	if err := resp.WriteEntity(apiVersions); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1Discovery(_ *restful.Request, resp *restful.Response) {
	if err := resp.WriteEntity(corev1APIResourceList); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apisDiscovery(req *restful.Request, resp *restful.Response) {
	if req.PathParameter("group") != "" {
		if req.PathParameter("group") == "rbac.authorization.k8s.io" && req.PathParameter("version") == "v1" {
			if err := resp.WriteEntity(rbav1APIResourceList); err != nil {
				_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
				return
			}
			return
		}
		_ = resp.WriteErrorString(http.StatusInternalServerError, fmt.Sprintf("discovery info not defined for %s/%s", req.PathParameter("group"), req.PathParameter("version")))
		return
	}

	if err := resp.WriteEntity(apiGroupList); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1Watch(req *restful.Request, resp *restful.Response) {
	// Watch opens a long-running connection that returns a stream of events to the connection.

}

func (h *apiServerHandler) apiV1Create(req *restful.Request, resp *restful.Response) {
	ctx := req.Request.Context()

	// Gets the resource group the request targets to (the resolver is aware of the mapping host<->resourceGroup)
	resourceGroup, err := h.resourceGroupResolver(req.Request.Host)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets at client to the resource group.
	cloudClient := h.manager.GetResourceGroup(resourceGroup).GetClient()

	// Maps the requested resource to a gvk.
	gvk, err := requestToGVK(req)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets the obj from the request.
	defer func() { _ = req.Request.Body.Close() }()
	objData, _ := io.ReadAll(req.Request.Body)

	newObj, err := h.manager.GetScheme().New(*gvk)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	codecFactory := serializer.NewCodecFactory(h.manager.GetScheme())
	if err := runtime.DecodeInto(codecFactory.UniversalDecoder(), objData, newObj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Create the object
	obj := newObj.(client.Object)
	// TODO: consider check vs enforce for namespace on the object - namespace on the request path
	obj.SetNamespace(req.PathParameter("namespace"))
	if err := cloudClient.Create(ctx, obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	if err := resp.WriteEntity(obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1List(req *restful.Request, resp *restful.Response) {
	ctx := req.Request.Context()

	// Gets the resource group the request targets to (the resolver is aware of the mapping host<->resourceGroup)
	resourceGroup, err := h.resourceGroupResolver(req.Request.Host)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets at client to the resource group.
	cloudClient := h.manager.GetResourceGroup(resourceGroup).GetClient()

	h.log.Info(fmt.Sprintf("[DEBUG] Serving List for %v", req.Request.URL))

	// Maps the requested resource to a gvk.
	gvk, err := requestToGVK(req)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	// If the request is for a watch it will be handled separately.
	if isWatch(req) {
		err = h.watchForResource(ctx, cloudClient, *gvk, resourceGroup, req, resp)
		if err != nil {
			_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
			return
		}
		return
	}

	// Reads and returns the requested data.
	list := &unstructured.UnstructuredList{}
	list.SetAPIVersion(gvk.GroupVersion().String())
	list.SetKind(fmt.Sprintf("%sList", gvk.Kind))

	listOpts := []client.ListOption{}
	if req.PathParameter("namespace") != "" {
		listOpts = append(listOpts, client.InNamespace(req.PathParameter("namespace")))
	}

	if err := cloudClient.List(ctx, list, listOpts...); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	if err := resp.WriteEntity(list); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1Get(req *restful.Request, resp *restful.Response) {
	ctx := req.Request.Context()

	// Gets the resource group the request targets to (the resolver is aware of the mapping host<->resourceGroup)
	resourceGroup, err := h.resourceGroupResolver(req.Request.Host)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets at client to the resource group.
	cloudClient := h.manager.GetResourceGroup(resourceGroup).GetClient()

	// Maps the requested resource to a gvk.
	gvk, err := requestToGVK(req)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Reads and returns the requested data.
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(gvk.GroupVersion().String())
	obj.SetKind(gvk.Kind)
	obj.SetName(req.PathParameter("name"))
	obj.SetNamespace(req.PathParameter("namespace"))

	if err := cloudClient.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if status, ok := err.(apierrors.APIStatus); ok || errors.As(err, &status) {
			_ = resp.WriteHeaderAndEntity(int(status.Status().Code), status)
			return
		}
		_ = resp.WriteHeaderAndEntity(http.StatusInternalServerError, err.Error())
		return
	}
	if err := resp.WriteEntity(obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1Update(req *restful.Request, resp *restful.Response) {
	ctx := req.Request.Context()

	// Gets the resource group the request targets to (the resolver is aware of the mapping host<->resourceGroup)
	resourceGroup, err := h.resourceGroupResolver(req.Request.Host)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets at client to the resource group.
	cloudClient := h.manager.GetResourceGroup(resourceGroup).GetClient()

	// Maps the requested resource to a gvk.
	gvk, err := requestToGVK(req)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets the obj from the request.
	defer func() { _ = req.Request.Body.Close() }()
	objData, _ := io.ReadAll(req.Request.Body)

	newObj, err := h.manager.GetScheme().New(*gvk)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	codecFactory := serializer.NewCodecFactory(h.manager.GetScheme())
	if err := runtime.DecodeInto(codecFactory.UniversalDecoder(), objData, newObj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Create the object
	obj := newObj.(client.Object)
	// TODO: consider check vs enforce for namespace on the object - namespace on the request path
	obj.SetNamespace(req.PathParameter("namespace"))
	if err := cloudClient.Update(ctx, obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	if err := resp.WriteEntity(obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1Patch(req *restful.Request, resp *restful.Response) {
	ctx := req.Request.Context()

	// Gets the resource group the request targets to (the resolver is aware of the mapping host<->resourceGroup)
	resourceGroup, err := h.resourceGroupResolver(req.Request.Host)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets at client to the resource group.
	cloudClient := h.manager.GetResourceGroup(resourceGroup).GetClient()

	// Maps the requested resource to a gvk.
	gvk, err := requestToGVK(req)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets the patch from the request
	defer func() { _ = req.Request.Body.Close() }()
	patchData, _ := io.ReadAll(req.Request.Body)
	patchType := types.PatchType(req.HeaderParameter("Content-Type"))
	patch := client.RawPatch(patchType, patchData)

	// Applies the Patch.
	obj := &unstructured.Unstructured{}
	// TODO: consider check vs enforce for gvk on the object - gvk on the request path (same for name/namespace)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	obj.SetKind(gvk.Kind)
	obj.SetName(req.PathParameter("name"))
	obj.SetNamespace(req.PathParameter("namespace"))

	if err := cloudClient.Patch(ctx, obj, patch); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	if err := resp.WriteEntity(obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1Delete(req *restful.Request, resp *restful.Response) {
	ctx := req.Request.Context()

	// Gets the resource group the request targets to (the resolver is aware of the mapping host<->resourceGroup)
	resourceGroup, err := h.resourceGroupResolver(req.Request.Host)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Gets at client to the resource group.
	cloudClient := h.manager.GetResourceGroup(resourceGroup).GetClient()

	// Maps the requested resource to a gvk.
	gvk, err := requestToGVK(req)
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Reads and returns the requested data.
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(gvk.GroupVersion().String())
	obj.SetKind(gvk.Kind)
	obj.SetName(req.PathParameter("name"))
	obj.SetNamespace(req.PathParameter("namespace"))

	if err := cloudClient.Delete(ctx, obj); err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
}

func (h *apiServerHandler) apiV1PortForward(req *restful.Request, resp *restful.Response) {
	// In order to handle a port forward request the current connection has to be upgraded
	// in order to become compliant with the spyd protocol.
	// This implies two steps:
	// - Adding support for handling multiple http streams, used for subsequent operations over
	//   the forwarded connection.
	// - Opening a connection to the target endpoint, the endpoint to port forward to, and setting up
	//   a bi-directional copy of data because the server acts as a man in the middle.

	podName := req.PathParameter("name")
	podNamespace := req.PathParameter("namespace")

	// Perform a sub protocol negotiation, ensuring tha client and the server agree on how
	// to handle communications over the port forwarded connection.
	request := req.Request
	respWriter := resp.ResponseWriter
	_, err := httpstream.Handshake(request, respWriter, []string{portforward.PortForwardProtocolV1Name})
	if err != nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	// Create a channel where to handle http streams that will be generated for each subsequent
	// operations over the port forwarded connection.
	streamChan := make(chan httpstream.Stream, 1)

	// Upgrade the connection specifying what to do when a new http stream is received.
	// After being received, the new stream will be published into the stream channel for handling.
	upgrader := spdy.NewResponseUpgrader()
	conn := upgrader.UpgradeResponse(respWriter, request, gportforward.HTTPStreamReceived(streamChan))
	if conn == nil {
		_ = resp.WriteErrorString(http.StatusInternalServerError, "failed to get upgraded connection")
		return
	}
	defer func() {
		_ = conn.Close()
	}()
	conn.SetIdleTimeout(10 * time.Minute)

	// Start the process handling streams that are published in the stream channel, please note that:
	// - The connection with the target will be established only when the first operation will be executed
	// - Following operations will re-use the same connection.
	streamHandler := gportforward.NewHTTPStreamHandler(
		conn,
		streamChan,
		podName,
		podNamespace,
		func(ctx context.Context, podName, podNamespace, _ string, stream io.ReadWriteCloser) error {
			// Given that in the goofy provider there is no real infrastructure, and thus no real workload cluster,
			// we are going to forward all the connection back to the same server (the goofy controller pod).
			return h.doPortForward(ctx, req.Request.Host, stream)
		},
	)
	streamHandler.Run(context.Background())
}

// doPortForward establish a connection to the target of the port forward operation,  and sets up
// a bi-directional copy of data.
// In the case of this provider, the target endpoint is always on the same server (the goofy controller pod).
func (h *apiServerHandler) doPortForward(ctx context.Context, address string, stream io.ReadWriteCloser) error {
	// Get a connection to the target of the port forward operation.
	dial, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to dial \"%s\": %w", address, err)
	}
	defer func() {
		_ = dial.Close()
	}()

	// Create a tunnel for bi-directional copy of data between the stream
	// originated from the initiator of the port forward operation and the target.
	return gportforward.HTTPStreamTunnel(ctx, stream, dial)
}

func (h *apiServerHandler) healthz(_ *restful.Request, resp *restful.Response) {
	resp.WriteHeader(http.StatusOK)
}

func requestToGVK(req *restful.Request) (*schema.GroupVersionKind, error) {
	resourceList := getAPIResourceList(req)
	if resourceList == nil {
		return nil, errors.Errorf("No APIResourceList defined for %s", req.PathParameters())
	}
	gv, err := schema.ParseGroupVersion(resourceList.GroupVersion)
	if err != nil {
		panic(fmt.Sprintf("invalid group version in APIResourceList: %s", resourceList.GroupVersion))
	}

	resource := req.PathParameter("resource")
	for _, r := range resourceList.APIResources {
		if r.Name == resource {
			gvk := gv.WithKind(r.Kind)
			return &gvk, nil
		}
	}
	return nil, errors.Errorf("Resource %s is not defined in the APIResourceList for %s", resource, req.PathParameters())
}

func getAPIResourceList(req *restful.Request) *metav1.APIResourceList {
	if req.PathParameter("group") != "" {
		if req.PathParameter("group") == "rbac.authorization.k8s.io" && req.PathParameter("version") == "v1" {
			return rbav1APIResourceList
		}
		return nil
	}
	return corev1APIResourceList
}

func isWatch(req *restful.Request) bool {
	return req.QueryParameter("watch") == "true"
}

func (h *apiServerHandler) watchForResource(ctx context.Context, cloudClient cclient.Client, gvk schema.GroupVersionKind, resourceGroup string, req *restful.Request, resp *restful.Response) error {
	c := h.manager.GetCache()
	i, err := c.GetInformerForKind(ctx, gvk)
	if err != nil {
		return err
	}
	h.log.Info(fmt.Sprintf("[DEBUG] Serving Watch for %v", req.Request.URL))
	events := make(chan *watch.Event)
	watcher := &WatchEventDispatcher{
		resourceGroup: resourceGroup,
		events:        events,
	}

	if err := i.AddEventHandler(watcher); err != nil {
		return err
	}
	watcher.Run(*req.Request, gvk, resp, h)
	return nil
}

type WatchEventDispatcher struct {
	resourceGroup string
	events        chan (*watch.Event)
}

func (m *WatchEventDispatcher) OnCreate(resourceGroup string, o client.Object) {
	if resourceGroup != m.resourceGroup {
		return
	}
	m.events <- &watch.Event{
		Type:   watch.Added,
		Object: o,
	}
}

func (m *WatchEventDispatcher) OnUpdate(resourceGroup string, old, new client.Object) {
	if resourceGroup != m.resourceGroup {
		return
	}
	m.events <- &watch.Event{
		Type:   watch.Modified,
		Object: new,
	}
}

func (m *WatchEventDispatcher) OnDelete(resourceGroup string, o client.Object) {
	if resourceGroup != m.resourceGroup {
		return
	}
	m.events <- &watch.Event{
		Type:   watch.Deleted,
		Object: o,
	}
}

func (m *WatchEventDispatcher) OnGeneric(resourceGroup string, o client.Object) {
	if resourceGroup != m.resourceGroup {
		return
	}
	m.events <- &watch.Event{
		Type:   watch.EventType("GENERIC"),
		Object: o,
	}
}

// Run serves a series of encoded events via HTTP with Transfer-Encoding: chunked
// or over a websocket connection.
func (m *WatchEventDispatcher) Run(req http.Request, gvk schema.GroupVersionKind, w http.ResponseWriter, h *apiServerHandler) {
	h.log.Info(fmt.Sprintf("[DEBUG] Starting a watch %v", req))

	flusher, ok := w.(http.Flusher)
	if !ok {
		fmt.Printf("unable to start watch - can't get http.Flusher: %#v", w)
		return
	}
	resp, ok := w.(*restful.Response)
	if !ok {
		fmt.Printf("unable to start watch - can't get restfule.Response: %#v", w)
		return
	}
	//TODO: Add timeout
	// begin the stream
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	var unknown runtime.Unknown

	//var unknown runtime.Unknown
	//internalEvent := &metav1.InternalEvent{}
	//outEvent := &metav1.WatchEvent{}
	//buf := []byte{}
	done := req.Context().Done()

	//codecs := serializer.NewCodecFactory(scheme.Scheme)
	//info, match := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), "application/json")
	//if !match {
	//	panic("failed to get SerializerInfo for application/yaml")
	//}
	//
	//encoder := codecs.EncoderForVersion(info.Serializer, gvk.GroupVersion())
	//e := streaming.NewEncoder(resp, encoder)
	for {
		select {
		case <-done:
			return
		case event, ok := <-m.events:
			h.log.Info(fmt.Sprintf("[DEBUG] Writing event %v", spew.Sprint(event)))
			if !ok {
				// End of results.
				return
			}
			b, err := json.Marshal(event.Object)
			if err != nil {
				panic(0)
			}
			unknown.Raw = b

			event.Object = &unknown
			if err := resp.WriteEntity(event.Object); err != nil {
				_ = resp.WriteErrorString(http.StatusInternalServerError, err.Error())
				return
			}
			if len(m.events) == 0 {
				flusher.Flush()
			}

			//if err = e.Encode(event.Object); err != nil {
			//	// unexpected error
			//	fmt.Printf("unable to encode watch object %T: %v", event.Object, err)
			//	return
			//}
			//
			//// ContentType is not required here because we are defaulting to the serializer
			//// type
			//unknown.Raw = buf
			//event.Object = &unknown
			//
			//*outEvent = metav1.WatchEvent{}
			//
			//// create the external type directly and encode it.  Clients will only recognize the serialization we provide.
			//// The internal event is being reused, not reallocated so its just a few extra assignments to do it this way
			//// and we get the benefit of using conversion functions which already have to stay in sync
			//*internalEvent = metav1.InternalEvent(*event)
			//err := metav1.Convert_v1_InternalEvent_To_v1_WatchEvent(internalEvent, outEvent, nil)
			//if err != nil {
			//	fmt.Printf("unable to convert watch object: %v", err)
			//	// client disconnect.
			//	return
			//}
			//if err := e.Encode(outEvent); err != nil {
			//	fmt.Printf("unable to encode watch object %T: %v", outEvent, err)
			//	// client disconnect.
			//	return
			//}
			//buf = []byte{}
		}
	}
}
