package telecleaner

import (
	"context"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/4spacesdk/telecleaner/pkg/kube"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	// Using vendor auth package
	// ref: https://github.com/kubernetes/client-go/issues/242
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/avast/retry-go"
	"github.com/mitchellh/go-homedir"
	"github.com/olekukonko/tablewriter"
)

type Telecleaner struct {
	kubernetes                   *kube.Kubernetes
	namespaces                   []string
	concurrency                  int
	ignorerablePodStartTimeOfSec int
	verbose                      bool
}

func NewByKubeConfig(c *Config) (*Telecleaner, error) {
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		d, err := homedir.Dir()
		if err != nil {
			return nil, err
		}
		path = fmt.Sprintf("%s/.kube/config", d)
	}
	k, err := kube.NewByKubeConfig(path)
	if err != nil {
		return nil, err
	}
	return new(c, k)
}

func NewByInClusterConfig(c *Config) (*Telecleaner, error) {
	k, err := kube.NewByInClusterConfig()
	if err != nil {
		return nil, err
	}
	return new(c, k)
}

func (te *Telecleaner) EnableVerbose() {
	te.verbose = true
}

func (te *Telecleaner) SetNamespaces(namespaces []string) {
	te.namespaces = namespaces
	te.debug(fmt.Sprintf("telecleaner.SetNamespaces() %v", namespaces))
}

func (te *Telecleaner) SetAllNamespaces() error {
	namespaces, err := te.getAllNamespaceNames()
	if err != nil {
		return err
	}
	te.namespaces = namespaces
	te.debug(fmt.Sprintf("telecleaner.SetAllNamespaces() %v", namespaces))
	return nil
}

func (te *Telecleaner) Get() error {
	rr, err := te.get()
	if err != nil {
		return err
	}

	if len(rr) == 0 {
		return nil
	}

	data := [][]string{}
	for _, r := range rr {
		data = append(data, []string{
			r.pod.Namespace,
			strconv.FormatBool(r.status),
			r.pod.Name,
		})
	}

	te.printTable([]string{
		"namespace",
		"status",
		"pod",
	}, data)

	return nil
}

func (te *Telecleaner) getAllNamespaceNames() ([]string, error) {
	namespaceList, err := te.kubernetes.Clientset.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	namespaceNames := []string{}
	for _, namespace := range namespaceList.Items {
		namespaceNames = append(namespaceNames, namespace.Name)
	}

	return namespaceNames, nil
}

func (te *Telecleaner) get() ([]resouce, error) {
	pods, err := te.getPods()
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, te.concurrency)
	rr := []resouce{}

	for _, pod := range pods {
		sem <- struct{}{}
		wg.Add(1)

		go func(pod corev1.Pod) {
			defer wg.Done()
			defer func() { <-sem }()

			// TODO: refactor
			status := false
			_ = retry.Do(
				func() error {
					status, err = te.getPodStatus(pod)
					if err != nil {
						te.error(fmt.Sprintf("telecleaner.getPodStatus() got error, pod: %s %s", pod.Name, err.Error()))
						return err
					}
					if !status {
						te.debug(fmt.Sprintf("telecleaner.getPodStatus() was false, pod: %s", pod.Name))
						return fmt.Errorf("telecleaner.getPodStatus() was false, pod: %s", pod.Name)
					}
					return nil
				},
				retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
					return time.Duration(n) * time.Second
				}),
				retry.Attempts(3),
			)
			r := resouce{
				pod:    pod,
				status: status,
			}
			rr = append(rr, r)
		}(pod)
	}

	wg.Wait()

	return rr, nil
}

func (te *Telecleaner) info(s string) {
	fmt.Printf("%s %s\n", time.Now(), s)
}

func (te *Telecleaner) error(s string) {
	fmt.Printf("%s [ERROR] %s\n", time.Now(), s)
}

func (te *Telecleaner) debug(s string) {
	if te.verbose {
		fmt.Printf("%s [DEBUG] %s\n", time.Now(), s)
	}
}

func (te *Telecleaner) cleanup(dryrun bool) error {
	rr, err := te.get()
	if err != nil {
		return err
	}

	dryrunMessage := ""
	if dryrun {
		dryrunMessage = "(dryrun)"
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, te.concurrency)

	for _, r := range rr {
		if r.status {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(pod corev1.Pod) {
			defer wg.Done()
			defer func() { <-sem }()

			te.info(fmt.Sprintf("Cleanup: %s/%s %s", pod.Namespace, pod.Name, dryrunMessage))

			err := te.cleanupOne(pod, dryrun)
			if err != nil {
				te.error(err.Error())
			}
		}(r.pod)
	}

	wg.Wait()

	return nil
}

func (te *Telecleaner) Cleanup(dryrun bool) error {
	return te.cleanup(dryrun)
}

func (te *Telecleaner) WatchWithCleanup(dryrun bool, intervalSec int) error {
	for range time.Tick(time.Duration(intervalSec) * time.Second) {
		if err := te.cleanup(dryrun); err != nil {
			return err
		}
	}
	return nil
}

func new(c *Config, kubernetes *kube.Kubernetes) (*Telecleaner, error) {
	return &Telecleaner{
		kubernetes:                   kubernetes,
		concurrency:                  c.Concurrency,
		ignorerablePodStartTimeOfSec: c.IgnorerablePodStartTimeOfSec,
		namespaces:                   []string{"default"},
		verbose:                      false,
	}, nil
}

func (te *Telecleaner) getPods() ([]corev1.Pod, error) {
	pods := []corev1.Pod{}
	for _, namespace := range te.namespaces {
		te.debug(fmt.Sprintf("telecleaner.getPods() namespace: %s", namespace))
		podList, err := te.kubernetes.Clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "telepresence",
		})
		if err != nil {
			return nil, err
		}

		for _, pod := range podList.Items {
			pods = append(pods, pod)
		}
	}

	return pods, nil
}

func (te *Telecleaner) checkPassageOfPodStartTime(now time.Time, podStartTime *metav1.Time) bool {
	thresholdTime := podStartTime.Add(time.Duration(te.ignorerablePodStartTimeOfSec) * time.Second)
	if now.Unix() > thresholdTime.Unix() {
		return true
	}

	return false
}

func (te *Telecleaner) getPodStatus(pod corev1.Pod) (bool, error) {
	te.debug(fmt.Sprintf("telecleaner.getPodStatus() %s", pod.Name))

	if !te.checkPassageOfPodStartTime(time.Now(), pod.Status.StartTime) {
		te.debug(fmt.Sprintf("The Pod will skip the check because it is not passage since the start %d seconds: %s", te.ignorerablePodStartTimeOfSec, pod.Name))
		return true, nil
	}

	containerName := ""
	for _, c := range pod.Spec.Containers {
		if strings.Contains(c.Image, "telepresence-k8s") {
			containerName = c.Name
			break
		}
	}
	if containerName == "" {
		return false, fmt.Errorf("Can't find telepresence container name in Pod: %s", pod.Name)
	}

	var stdout, stderr bytes.Buffer
	req := te.kubernetes.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		Param("container", containerName)
	req.VersionedParams(&corev1.PodExecOptions{
		Command: []string{"sh", "-c", "echo -en 'HEAD / HTTP/1.1\n\n' | nc localhost 9055; exit 0"},
		Stdin:   false,
		Stdout:  true,
		Stderr:  false,
		TTY:     true,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(te.kubernetes.RestConfig, "POST", req.URL())
	if err != nil {
		return false, err
	}

	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return false, err
	}

	if stdout.String() != "" {
		te.debug(stdout.String())
	}

	if strings.Contains(stdout.String(), "HTTP/1.0 200 OK") {
		return true, nil
	}

	return false, nil
}

func (te *Telecleaner) cleanupOne(pod corev1.Pod, dryrun bool) error {
	te.debug(fmt.Sprintf("telecleaner.cleanupOne() %s, dryrun: %t", pod.Name, dryrun))

	replicaSetName := ""
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "ReplicaSet" {
			replicaSetName = ref.Name
			break
		}
	}
	if replicaSetName == "" {
		return fmt.Errorf("Can't find ReplicaSet name in OwnerReferences: %s", pod.Name)
	}

	replicaSet, err := te.kubernetes.Clientset.AppsV1().ReplicaSets(pod.Namespace).Get(context.TODO(), replicaSetName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	deploymentName := ""
	for _, ref := range replicaSet.OwnerReferences {
		if ref.Kind == "Deployment" {
			deploymentName = ref.Name
			break
		}
	}
	if deploymentName == "" {
		return fmt.Errorf("Can't find Deployment name in OwnerReferences: %s", pod.Name)
	}

	deploymentClientset := te.kubernetes.Clientset.AppsV1().Deployments(pod.Namespace)

	deployment, err := deploymentClientset.Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}

    return fmt.Errorf("Label? %s", deployment.Labels["telepresence"]);

	lac := KubernetesLastAppliedConfiguration{}
	json.Unmarshal(([]byte)(deployment.Annotations["kubectl.kubernetes.io/last-applied-configuration"]), &lac)
	if lac.Metadata.SelfLink == "" {
		return fmt.Errorf("Can't find original Deployment name in annotations: %s", pod.Name)
	}

	originalDeployment, err := deploymentClientset.Get(context.TODO(), filepath.Base(lac.Metadata.SelfLink), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if !dryrun {
		newOriginalDeployment := originalDeployment
		newOriginalDeployment.Spec.Replicas = &lac.Spec.Replicas
		_, err = deploymentClientset.Update(context.TODO(), newOriginalDeployment, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		if err := deploymentClientset.Delete(context.TODO(), deployment.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}

	return nil
}

func (te *Telecleaner) printTable(header []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding(" ")
	table.SetNoWhiteSpace(true)
	table.AppendBulk(data)
	table.Render()
}
