package main

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func CheckAndCreateTopic(adminClient *admin.Client, topicName string) (*admin.CreateTopicResponse, error) {
	// Check if the topic exists
	topic, err := adminClient.GetTopic(context.Background(), topicName, nil)
	if err != nil {
		fmt.Printf("GetTopic(%s) error: %v\n", topicName, err)
		return nil, err
	}
	if topic != nil {
		// Topic exists
		return nil, fmt.Errorf("Topic '%s' already exists.\n", topicName)
	}

	// If the topic does not exist, create it
	resp, err := adminClient.CreateTopic(context.Background(), topicName, nil)
	if err != nil {
		log.Fatalf("Failed to create topic '%s': %v", topicName, err)
		return nil, err
	}

	fmt.Printf("Topic '%s' created successfully.\n", topicName)
	return &resp, nil
}

type SharedAccessAuthorizationRule struct {
	XMLName xml.Name `xml:"SharedAccessAuthorizationRule"`
	Name    string   `xml:"Name"`
	Rights  []string `xml:"Rights>AccessRights"`
}

func GenerateRandomString() string {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(fmt.Sprintf("Error generating random string: %v", err))
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

func CreateSAPForTopic(adminClient *admin.Client, topic *admin.CreateTopicResponse, topicName string, sapName string) error {
	accessRight := "Listen"
	primaryKey := GenerateRandomString()
	secondaryKey := GenerateRandomString()

	now := time.Now()
	authRule := &admin.AuthorizationRule{
		KeyName:      &sapName,
		AccessRights: []admin.AccessRight{admin.AccessRight(accessRight)},
		CreatedTime:  &now,
		ModifiedTime: &now,
		PrimaryKey:   &primaryKey,
		SecondaryKey: &secondaryKey,
	}
	topic.AuthorizationRules = append(topic.AuthorizationRules, *authRule)

	_, err := adminClient.UpdateTopic(context.Background(), topicName, admin.TopicProperties{
		MaxSizeInMegabytes:                  topic.MaxSizeInMegabytes,
		RequiresDuplicateDetection:          topic.RequiresDuplicateDetection,
		DefaultMessageTimeToLive:            topic.DefaultMessageTimeToLive,
		DuplicateDetectionHistoryTimeWindow: topic.DuplicateDetectionHistoryTimeWindow,
		EnableBatchedOperations:             topic.EnableBatchedOperations,
		Status:                              topic.Status,
		AutoDeleteOnIdle:                    topic.AutoDeleteOnIdle,
		EnablePartitioning:                  topic.EnablePartitioning,
		SupportOrdering:                     topic.SupportOrdering,
		UserMetadata:                        topic.UserMetadata,
		AuthorizationRules:                  topic.AuthorizationRules,
		MaxMessageSizeInKilobytes:           topic.MaxMessageSizeInKilobytes,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func DeleteTopic(adminClient *admin.Client, topicName string) {
	// Check if the topic exists
	topic, err := adminClient.GetTopic(context.Background(), topicName, nil)
	if err != nil {
		fmt.Printf("GetTopic(%s) error: %v\n", topicName, err)
	}
	if topic == nil {
		// Topic does not exist
		fmt.Printf("Topic '%s' does not exist.\n", topicName)
		return
	}

	// If the topic exists, delete it)
	_, err = adminClient.DeleteTopic(context.Background(), topicName, nil)
	if err != nil {
		log.Fatalf("Failed to delete topic '%s': %v", topicName, err)
	}

	fmt.Printf("Topic '%s' deleted successfully.\n", topicName)
}

func main() {
	// Replace this with your Azure Service Bus connection string
	connectionString := ""

	// Create a new Service Bus administration client using the connection string
	adminClient, err := admin.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Fatalf("Failed to create a Service Bus admin client: %v", err)
	}

	const maxConcurrency = 500 // Set the max number of concurrent goroutines
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	// Loop to create 10,000 topics
	for i := 1; i <= 10000; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }() // Release the spot in the semaphore

			topicName := "vj-test-" + strconv.Itoa(i)
			sapName := "vj-test-sap-" + strconv.Itoa(i)

			topic, err := CheckAndCreateTopic(adminClient, topicName)
			if err != nil {
				log.Printf("Failed to create topic '%s': %v", topicName, err)
				return
			}

			if err := CreateSAPForTopic(adminClient, topic, topicName, sapName); err != nil {
				log.Printf("Failed to create SAP for topic '%s': %v", topicName, err)
			}
		}(i)
	}

	wg.Wait()
	//createSharedAccessPolicy(connectionString, "test-topic-100", "test-policy", []string{"Listen"})
	//CreateSAPTopic(adminClient, "test-topic-100")
	//CheckAndCreateTopic(adminClient, "test-topic-100")
}
