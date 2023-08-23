package client

import (
	"bytes"
	"strings"
	"strconv"
	"net/http"
	"io/ioutil"
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

func GetKeyResourceVersion(obj runtime.Object) (string, int64) {
	key := GetObjectKey(obj)
	accessor, err := meta.Accessor(obj)

	if err != nil || key == "" || len(accessor.GetResourceVersion()) == 0 {
		return key, 0
	}

	resourceVersion, _ := strconv.ParseUint(accessor.GetResourceVersion(), 10, 64)

	return key, int64(resourceVersion)
}

func GetObjectKey(obj runtime.Object) string {
	accessor, err := meta.Accessor(obj)
	if err != nil || accessor.GetName() == "" {
		return ""
	}
	objectGroup := strings.ToLower(obj.GetObjectKind().GroupVersionKind().Group)
	objectKind := strings.ToLower(obj.GetObjectKind().GroupVersionKind().Kind)

	key := "/registry/"

	// Support more K8s Objects
	switch objectKind {
	case "statefulset":
		key += objectKind + "s/"
	case "deployment":
		key += objectKind + "s/"
	case "poddisruptionbudget":
		key += objectKind + "s/"
	case "configmap":
		key += objectKind + "s/"
	case "service":
		key += objectKind + "s/specs/" 
	case "":
		key += "persistentvolumeclaims"
	// CRD
	default:
		key += objectGroup + "/" + objectKind + "s/"
	}
	key += accessor.GetNamespace() + "/" + accessor.GetName()

	return key
}

func IsCRD(obj runtime.Object) bool {
	kind := strings.ToLower(obj.GetObjectKind().GroupVersionKind().Kind)
	if kind == "pod" || kind == "replicaset" || kind == "deployment" ||
		kind == "statefulset" || kind == "daemonset" || kind == "job" ||
		kind == "cronjob" || kind == "service" || kind == "ingress" ||
		kind == "configmap" || kind == "secret" || kind == "namespace" ||
		kind == "persistentvolumeclaim" || kind == "persistentvolume" ||
		kind == "resourcequota" || kind == "serviceaccount" || kind == "role" ||
		kind == "horizontalpodautoscaler" || kind == "verticalpodautoscaler" ||
		kind == "clusterrole" || kind == "clusterrolebinding" ||
		kind == "poddisruptionbudget" {
			return false
		}
	return true
}

func PutCompare(compares *map[string]int64, key string, rv int64) {
	// Do nothing if a key doesn't exsit or pvc.
	if key == "" || strings.Contains(key, "persistentvolumeclaim") {
		return
	}

	// Keep an old value if a key already exists.
	_, exist := (*compares)[key]
	if !exist {
		(*compares)[key] = rv
	}
}

func PutRequest(requests *map[string]string, key string, value string) {
	// Do nothing if a key doesn't exsit.
	if key == "" {
		return
	}

	origValue, exist := (*requests)[key]
	if exist {
		// Deleted before
		if origValue == "" {
			return
		}
		// Updated/Created before
		// TODO: implement overriding logic
	} else {
		(*requests)[key] = value
	}
}

const TXN_MANAGER_URL = "http://127.0.0.1:8000/txn"

func TxnRequest(compares map[string]int64, requests map[string]string) bool {
	txnData := map[string]interface{}{
		"compares": compares,
		"requests": requests,
	}

	txnRequest, err := json.Marshal(txnData)
	if err != nil {
		return false
	}

	resp, err := http.Post(TXN_MANAGER_URL, "application/json", bytes.NewBuffer(txnRequest))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)

	if strings.Contains(string(body), "Succeed") {
		return true
	}
	return false
}