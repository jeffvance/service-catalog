/*
Copyright 2016 The Kubernetes Authors.

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

package s3broker

import (
	"fmt"
	"sync"
	"net/http"
	"io/ioutil"
	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/pkg/api/v1"
	k8sRest "k8s.io/client-go/rest" 
	"github.com/kubernetes-incubator/service-catalog/contrib/pkg/brokers/broker"
	"github.com/kubernetes-incubator/service-catalog/pkg/brokerapi"
	"github.com/minio/minio-go"
)

// CreateBroker initializes the service broker. This function is called by server.Start()
func CreateBroker() broker.Broker {
	const S3_BROKER_POD_LABEL = "glusterfs=s3-pod"
	var instanceMap = make(map[string]*s3ServiceInstance)

	s3ip, err := getExternalIP()
	if err != nil {
		glog.Errorf("Failed to get external IP: %v", err)
	}

	// get the kubernetes client
	cs, err := getKubeClient()
	if err != nil {
		glog.Fatalf("failed to get kubernetes client: %v\n", err)
	}

	// get the s3 deployment pod created via the `gk-deploy` script
	// need to get this pod using label selectors since its name is generated
	ns := "default"
	podList, err := cs.CoreV1().Pods(ns).List(metav1.ListOptions{
		LabelSelector: S3_BROKER_POD_LABEL,
	})
	if err != nil || len(podList.Items) != 1 {
		glog.Fatalf("failed to get s3-deploy pod via label %q: %v\n", S3_BROKER_POD_LABEL, err)
	}
	s3Pod := podList.Items[0]

	// get user, account and password from s3 pod (supplied to the `gk-deloy` script)
	acct, user, pass := "", "", ""
	for _, pair := range s3Pod.Spec.Containers[0].Env {
		switch pair.Name {
		case "S3_ACCOUNT":
			acct = pair.Value
		case "S3_USER":
			user = pair.Value
		case "S3_PASSWORD":
			pass = pair.Value
		default:
			glog.Fatalf("unexpected env key %q for s3-deploy pod %q\n", pair.Name, s3Pod.Name)
		}
	}

	// get s3 deployment service in order to get the s3 broker's external port
	svcName := "gluster-s3-deployment"
	svc, err := cs.Services(ns).Get(svcName, metav1.GetOptions{})
	if err != nil {
		glog.Fatalf("failed to get s3 service %q: %v\n", svcName, err)
	}
	s3Port := svc.Spec.Ports[0].NodePort // int, TODO: always [0]?

	// get the s3 client
	s3c, err := getS3Client(acct, user, pass, s3ip)
	if err != nil {
		glog.Fatalf("failed to get minio-s3 client: %v\n", err)
	}

	glog.Infof("**** CreateBroker: s3Port=%v, s3url=%q\n", s3Port, fmt.Sprintf("%v:%v", s3ip, s3Port))
	return &s3Broker{
		instanceMap: instanceMap,
		s3Client: s3c,
		s3url: fmt.Sprintf("%v:%v", s3ip, s3Port),
		kubeClient: cs,
	}
}

type s3ServiceInstance struct {
	// k8s namespace
	Namespace string
	// binding credential created during Bind()
	Credential brokerapi.Credential // s3 server url, includes port and bucket name
}

type s3Broker struct {
	// rwMutex controls concurrent R and RW access
	rwMutex sync.RWMutex
	// instanceMap maps instanceIDs to the ID's userProvidedServiceInstance values
	instanceMap map[string]*s3ServiceInstance
	// client used to access s3 API
	s3Client *minio.Client
	// s3 server ip and port
	s3url string // includes ":port"
	// client used to access kubernetes
	kubeClient *clientset.Clientset
}

func (b *s3Broker) Catalog() (*brokerapi.Catalog, error) {
	return &brokerapi.Catalog{
		Services: []*brokerapi.Service{
			{
				Name:        "gluster-object-store",
				ID:          "lkdgkf2napdwedom",
				Description: "An object bucket backed by GlusterFS Object Storage.",
				Bindable:    true,
				Plans: []brokerapi.ServicePlan{
					{
						Name:        "default",
						ID:          "0",
						Description: "The best plan, and the only one.",
						Free:        true,
					},
				},
			},
		},
	}, nil
}

func (b *s3Broker) GetServiceInstanceLastOperation(instanceID, serviceID, planID, operation string) (*brokerapi.LastOperationResponse, error) {
	glog.Info("GetServiceInstanceLastOperation not yet implemented.")
	return nil, nil
}

func (b *s3Broker) CreateServiceInstance(instanceID string, req *brokerapi.CreateServiceInstanceRequest) (*brokerapi.CreateServiceInstanceResponse, error) {
	const BUCKET_URL = "bucketurl"

	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()

	// does service instance exist?
	if _, ok := b.instanceMap[instanceID]; ok {
		return nil, fmt.Errorf("ServiceInstance %q already exists", instanceID)
	}
	// create new service instance
	ns := req.ContextProfile.Namespace
	newSvcInstance := &s3ServiceInstance{
		Namespace: ns,
		Credential: nil, //TODO
	}
	glog.Infof("creating ServiceInstance %q", instanceID)

	// request parameters:
	bucketName, ok := req.Parameters["bktNameParm"].(string)
	if !ok {
		bucketName = "DemoBkt1" // default
	}
	glog.Infof("creating bucket %q...", bucketName)

	// provision requested bucket
	err := b.provisionBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("cannot provision bucket %q: %v", bucketName, err)
	}
	glog.Info("*** no err creating bucket %q", bucketName)

	newSvcInstance.Credential[BUCKET_URL] = fmt.Sprintf("%s/%s", b.s3url, bucketName)
	glog.Info("**** new instance: %+v", newSvcInstance)
	b.instanceMap[instanceID] = newSvcInstance
	glog.Info("**** full instanceMap: %+v", b.instanceMap)
	return nil, nil
}

func (b *s3Broker) RemoveServiceInstance(instanceID, serviceID, planID string, acceptsIncomplete bool) (*brokerapi.DeleteServiceInstanceResponse, error) {
	glog.Info("RemoveServiceInstance not yet implemented.")
	return nil, nil
}

func (b *s3Broker) Bind(instanceID, bindingID string, req *brokerapi.BindingRequest) (*brokerapi.CreateServiceBindingResponse, error) {
	instance, ok := b.instanceMap[instanceID]
	if ! ok {
		return nil, fmt.Errorf("Instance ID %q not found.", instanceID)
	}
	creds := instance.Credential
	if creds == nil {
		return nil, fmt.Errorf("No credentials found for instance %q.", instanceID)
	}
	return &brokerapi.CreateServiceBindingResponse{
		Credentials: creds,
	}, nil
}

// nothing to do here
func (b *s3Broker) UnBind(instanceID, bindingID, serviceID, planID string) error {
	glog.Info("UnBind not yet implemented.")
	return nil
}

func (b *s3Broker) provisionBucket(bucket string) error {
	location := "" // ignored for now...

	// check if bucket already exists
	exists, err := b.s3Client.BucketExists(bucket)
	if err == nil && exists {
		return fmt.Errorf("bucket %q already exists", bucket)
	}
	glog.Info("**** provisionBucket: bucket %q needs to be created", bucket)

	// create new bucket
	err = b.s3Client.MakeBucket(bucket, location)
	if err != nil {
		return fmt.Errorf("MakeBucket err: %v", err)
	}
	glog.Info("**** provisionBucket: bucket %q be was created", bucket)
	return nil
}

// getS3Client returns a minio api client
func getS3Client(acct, user, pass, ip string) (*minio.Client, error) {
	glog.Infof("Creating s3 client based on: \"%s:%s\" on ip %s", acct, user, ip)

	id := fmt.Sprintf("%s:%s", acct, user)
	useSSL := false
	minioClient, err := minio.NewV2(ip, id, pass, useSSL)
	if err != nil {
		return nil, fmt.Errorf("unable to create minio S3 client: %v", err)
	}
	return minioClient, nil
}

// getKubeClient returns a k8s api client
func getKubeClient() (*clientset.Clientset, error) {
	glog.Info("Getting k8s API Client config")
	kubeClientConfig, err := k8sRest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to create k8s in-cluster config: %v", err)
	}
	glog.Info("Creating new Kubernetes Clientset")
	cs, err := clientset.NewForConfig(kubeClientConfig)
	return cs, err
}

func getExternalIP() (string, error) {
	c := http.Client{}
	glog.Info("Requesting external IP.")
	req, err := http.NewRequest("GET", "http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip", nil)
	if err != nil {
		glog.Errorf("Failed to create new http request: %v", err)
		return "", err
	}
	req.Header.Add("Metadata-Flavor", " Google")
	resp, err := c.Do(req)
	if err != nil {
		glog.Errorf("Failed to sent http request: %v", err)
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Failed to decode http response body: %v", err)
		return "", err
	}
	glog.Infof("Got external ip: %v", string(body))
	return string(body), nil
}

