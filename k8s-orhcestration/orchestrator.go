package k8sorhcestration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"retl/inputs/types"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)


func sanitizeName(name string) string {
	re := regexp.MustCompile("[^a-z0-9-]+")
	sanitized := re.ReplaceAllString(strings.ToLower(name), "-")
	return sanitized
}

func RunOrchestration(connectorType string, connectorName string, conf *types.ConfigType, pipelineName string) error {
	fmt.Println("STARTING ORCHESTRATION")
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	clientcmd.BuildConfigFromFlags("", kubeconfig)
    config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
    if err != nil {
        return err
    }
    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        return err
    }
	
	envVariablesForSpec := []corev1.EnvVar{}
	envVariablesForSpec = append(envVariablesForSpec, corev1.EnvVar{
		Name: "CONNECTOR_NAME",
		Value: connectorName,
	})
	envVariablesForSpec = append(envVariablesForSpec, corev1.EnvVar{
		Name: "PIPELINE_NAME",
		Value: pipelineName,
	})
	for key, value := range conf.Settings {
		envVariablesForSpec = append(envVariablesForSpec, corev1.EnvVar{
			Name: key,
			Value: fmt.Sprintf("%v", value),
		})
	}
	for key, value := range conf.Secrets {
		envVariablesForSpec = append(envVariablesForSpec, corev1.EnvVar{
			Name: key,
			Value: fmt.Sprintf("%v", value),
		})
	}
	fmt.Println(envVariablesForSpec)
	fmt.Println(connectorType)
	var image string
	if connectorType == "Input" {
		image = "aneeshseth/inputs:latest"
	} else {
		image = "aneeshseth/outputs:latest"
	}
	fmt.Println(image)
	
	// Sanitize names for RFC compliance
	sanitizedConnectorType := sanitizeName(connectorType)
	sanitizedConnectorName := sanitizeName(connectorName)
	
	podConfig := &corev1.Pod{
        ObjectMeta: metav1.ObjectMeta{
            GenerateName: fmt.Sprintf("%s-%s", sanitizedConnectorType, sanitizedConnectorName),
            Namespace:    "default",
        },
        Spec: corev1.PodSpec{
            Containers: []corev1.Container{
                {
                    Name:  fmt.Sprintf("%s-%s", sanitizedConnectorType, sanitizedConnectorName),
					Image: image,
                    Env: envVariablesForSpec,
                },
            },
        },
    }
	podClient := clientset.CoreV1().Pods("default")
	_, err = podClient.Create(context.TODO(), podConfig, metav1.CreateOptions{})
	if err != nil {
		fmt.Println("ERRRRORRR")
		fmt.Println(err)
		return err
	}
	return nil
}
