package utils

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func GetTestKubenertesInterface() (kubernetes.Interface, error) {
	clientset := fake.NewSimpleClientset()
	return clientset, nil
}

func getKubernetesConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return config, nil

}

func GetKubernetesDynamicInterface() (dynamic.Interface, error) {
	clientConfig, err := getKubernetesConfig()
	if err != nil {
		return nil, err
	}

	// Create the dynamic client for the memberlist custom resource
	dynamic_client, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		panic(err.Error())
	}
	return dynamic_client, nil
}

func GetKubernetesInterface() (kubernetes.Interface, error) {
	config, err := getKubernetesConfig()
	if err != nil {
		return nil, err
	}
	// Create a clientset for the coordinator
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}
