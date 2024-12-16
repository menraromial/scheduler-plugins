package carbonaware

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	//"path"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)


const (
	podPrefix = "/registry/kcas-metrics/pods"
)

type EtcdClient struct {
	client *clientv3.Client
	//address string
}

type UsageEntry struct {
	Timestamp     string  `json:"timestamp"`
	MaxCPUUsage   float64 `json:"max_cpu_usage"`
	MaxMemoryUsage float64 `json:"max_memory_usage"`
	MaxEnergyUsage int `json:"max_energy_usage"`
}

type PodUsage struct {
	UsageHistory []UsageEntry `json:"usage_history"`
}

type EtcdData struct {
	Pods map[string]PodUsage `json:"pods"`
}


func NewEtcdClient(address string) *EtcdClient {
	// Charger le certificat CA
	caCert, err := os.ReadFile("/etc/kubernetes/pki/etcd/ca.crt")
	if err != nil {
		log.Fatalf("Erreur lors de la lecture du certificat CA : %v", err)
	}

	// Ajouter le certificat CA au pool de certificats
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Charger le certificat client et la clé privée
	cert, err := tls.LoadX509KeyPair("/etc/kubernetes/pki/etcd/server.crt", "/etc/kubernetes/pki/etcd/server.key")
	if err != nil {
		log.Fatalf("Erreur lors du chargement du certificat client : %v", err)
	}

	// Configurer TLS
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}

	// Configuration de la connexion à etcd
	etcdConfig := clientv3.Config{
		Endpoints:   []string{address}, // Adresse HTTPS du serveur etcd
		DialTimeout: 5 * time.Second,                      // Timeout de la connexion
		TLS:         tlsConfig,                            // Configuration TLS
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		log.Fatalf("Erreur lors de la connexion à etcd : %v", err)
	}

	return &EtcdClient{ client: client }
}

func (e *EtcdClient) getPodData(ctx context.Context, podName string) (*PodUsage, error) {

	key := fmt.Sprintf("%s/%s", podPrefix, podName)
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error retrieving data for pod %s: %v", podName, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("no data found for pod %s", podName)
	}

	var podUsage PodUsage
	err = json.Unmarshal(resp.Kvs[0].Value, &podUsage)
	if err != nil {
		return nil, fmt.Errorf("error deserializing data for pod %s: %v", podName, err)
	}

	return &podUsage, nil
}

func (e *EtcdClient) getStat(ctx context.Context, podName string) (int, float32, error) {
	podUsage, err := e.getPodData(ctx, podName)
	if err != nil {
		return 0, 0, err
	}

	if len(podUsage.UsageHistory) == 0 {
		return 0, 0, fmt.Errorf("no usage history found for pod %s", podName)
	}

	var maxEnergyUsage int
	var maxCPUUsage float64
	for _, usage := range podUsage.UsageHistory {
		if usage.MaxEnergyUsage > maxEnergyUsage {
			maxEnergyUsage = usage.MaxEnergyUsage
			maxCPUUsage = usage.MaxCPUUsage
		}
	}
	return maxEnergyUsage,float32(maxCPUUsage) , nil
}