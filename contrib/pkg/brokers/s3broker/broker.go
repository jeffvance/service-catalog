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

	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/pkg/api/v1"  //??
	k8sRest "k8s.io/client-go/rest" 
	"github.com/kubernetes-incubator/service-catalog/contrib/pkg/brokers/broker"
	"github.com/kubernetes-incubator/service-catalog/pkg/brokerapi"
	"github.com/minio/minio-go"
)

// CreateBroker initializes the service broker.  This function is called by server.Start()
func CreateBroker() broker.Broker {
	// TODO temp use of minio import so it can be vendored in.
	_, _ = minio.NewV2("", "", "", false)

	var instanceMap = make(map[string]*s3ServiceInstance)
	return &s3Broker{
		instanceMap: instanceMap,
	}
}

type s3ServiceInstance struct {
	// unique instance id
	Id string
	// k8s namespace
	Namespace string
	// id of the service class??
	ServiceID string
	// binding credential created during Bind()
	Credential *brokerapi.Credential // user, pwd, s3 url
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
	// s3 account, user and user-password strings
	objAccount string
	objUser string
	objPass string
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
	glog.Info("CreateServiceInstance", instanceID)

	if _, ok := b.instanceMap[instanceID]; ok {
		return nil, fmt.Errorf("ServiceInstance %q already exists", instanceID)
	}
	// create new service instance
	newSvcInstance := &s3ServiceInstance{
		Id: instanceID,
		ServiceID: req.ServiceID,
		Namespace: req.ContextProfile.Namespace,
		Credential: nil, //TODO
	}

	// request parameters:
	bucketName, ok := req.Parameters["bktNameParm"].(string)
	if !ok {
		bucketName = "DemoBkt1" // default
	}
	glog.Info("bucket", bucketName, "to be created...")

	// provision requested bucket
	err := doS3BucketProvision(bucketName, newSvcInstance.Namespace)
	//TODO: reference above clients! For now just log them...
	glog.Info("s3 client:", s3client, "k8s client:", k8sClient)

	b.instanceMap[instanceID] = newSvcInstance
	//TODO: return serviceInstanceResponse, nil
	return nil, nil
}

func (b *s3Broker) RemoveServiceInstance(instanceID, serviceID, planID string, acceptsIncomplete bool) (*brokerapi.DeleteServiceInstanceResponse, error) {
	glog.Info("RemoveServiceInstance not yet implemented.")
	return nil, nil
}

func (b *s3Broker) Bind(instanceID, bindingID string, req *brokerapi.BindingRequest) (*brokerapi.CreateServiceBindingResponse, error) {
	glog.Info("Bind not yet implemented.")
	return nil, nil
}
func (b *s3Broker) UnBind(instanceID, bindingID, serviceID, planID string) error {
	glog.Info("UnBind not yet implemented.")
	return nil
}

func doS3BucketProvision(bucket, ns string) error {
	// get the s3 client
	s3, err := getS3Client()
	if err != nil {
		return err
	}
	// get the kubernetes client
	cs, err := getKubeClient()
	if err != nil {
		return err
	}
	glog.Info(s3, cs)  //TODO: just to make compiler happy... for now

	return nil
}

// getS3Client returns a minio api client
func getS3Client() (*minio.Client, error) {
	glog.Info("Creating new S3 Client")
	endpoint := "play.minio.io:9000" // fix this!!!
	id := "Q3AM3UQ867SPQQA43P2F"     // fix
	key := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true
	minioClient, err := minio.NewV2(endpoint, id, key, useSSL)
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

