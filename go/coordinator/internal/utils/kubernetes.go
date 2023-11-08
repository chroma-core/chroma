package utils

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func GetTestKubenertesInterface() (kubernetes.Interface, error) {
	clientset := fake.NewSimpleClientset()
	return clientset, nil
}

func getKubernetesConfig() (*rest.Config, error) {
	// Load the default kubeconfig file
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := loadingRules.Load()
	if err != nil {
		return nil, err
	}

	clientConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}

	return clientConfig, nil
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
	// Load the default kubeconfig file
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := loadingRules.Load()
	if err != nil {
		return nil, err
	}

	clientConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}

	// Create a clientset for the coordinator
	clientset, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}
