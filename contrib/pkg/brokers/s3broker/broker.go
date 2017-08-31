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

	// get the kubernetes client
	cs, err := getKubeClient()
	if err != nil {
		glog.Fatalf("failed to get kubernetes client: %v", err)
	}

	// get the s3 deployment pod created via the `gk-deploy` script
	// need to get this pod using label selectors since its name is generated
	ns := "default"
	podList, err := cs.CoreV1().Pods(ns).List(metav1.ListOptions{
		LabelSelector: S3_BROKER_POD_LABEL,
	})
	if err != nil || len(podList.Items) != 1 {
		glog.Fatalf("failed to get s3-deploy pod via label %q: %v", S3_BROKER_POD_LABEL, err)
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
			glog.Fatalf("unexpected env key %q for s3-deploy pod %q", pair.Name, s3Pod.Name)
		}
	}

	// get s3 service in order to get the s3 broker's endpoint
	svcName := "gluster-s3-service"
	svc, err := cs.Services(ns).Get(svcName, metav1.GetOptions{})
	if err != nil {
		glog.Fatalf("failed to get s3 service %q: %v", svcName, err)
	}
	s3IP := svc.Spec.ClusterIP // minio endpoint

	// get the s3 client
	s3c, err := getS3Client(acct, user, pass, s3IP)
	if err != nil {
		glog.Fatalf("failed to get minio-s3 client: %v", err)
	}

	return &s3Broker{
		instanceMap: instanceMap,
		s3Client: s3c,
		kubeClient: cs,
	}
}

type s3ServiceInstance struct {
	// k8s namespace
	Namespace string
	// binding credential created during Bind()
	Credential *brokerapi.Credential // s3 server url, includes port and bucket name
}

type s3Broker struct {
	// rwMutex controls concurrent R and RW access
	rwMutex sync.RWMutex
	// instanceMap maps instanceIDs to the ID's userProvidedServiceInstance values
	instanceMap map[string]*s3ServiceInstance
	// client used to access s3 API
	s3Client *minio.Client
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
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	glog.Infof("CreateServiceInstance %q", instanceID)

	if _, ok := b.instanceMap[instanceID]; ok {
		return nil, fmt.Errorf("ServiceInstance %q already exists", instanceID)
	}
	// create new service instance
	newSvcInstance := &s3ServiceInstance{
		Namespace: req.ContextProfile.Namespace,
		Credential: nil, //TODO
	}

	// request parameters:
	bucketName, ok := req.Parameters["bktNameParm"].(string)
	if !ok {
		bucketName = "DemoBkt1" // default
	}
	glog.Infof("creating bucket %q...", bucketName)

	// provision requested bucket
	err := doS3BucketProvision(bucketName, newSvcInstance.Namespace)
	if err != nil {
		return nil, fmt.Errorf("cannot provision bucket %q", bucketName)
	}

	b.instanceMap[instanceID] = newSvcInstance
	//TODO: return serviceInstanceResponse, nil
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
	creds := *instance.Credential
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

func doS3BucketProvision(bucket, ns string) error {

	return nil
}

// getS3Client returns a minio api client
func getS3Client(acct, user, pass, ip string) (*minio.Client, error) {
	glog.Infof("Creating s3 client based on: %q:%q on ip %s", acct, user, ip)

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

