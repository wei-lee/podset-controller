package podset

import (
	"context"
	"fmt"
	appv1alpha1 "github.com/redhat/podset-operator/pkg/apis/app/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_podset")

const (
	POD_LABEL_NAME = "podsets.app.example.com/operator"
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new PodSet Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcilePodSet{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("podset-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource PodSet
	err = c.Watch(&source.Kind{Type: &appv1alpha1.PodSet{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner PodSet
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &appv1alpha1.PodSet{},
	})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcilePodSet{}

// ReconcilePodSet reconciles a PodSet object
type ReconcilePodSet struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a PodSet object and makes changes based on the state read
// and what is in the PodSet.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcilePodSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling PodSet")

	// Fetch the PodSet instance
	instance := &appv1alpha1.PodSet{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	if !instance.GetDeletionTimestamp().IsZero() {
		//this PodSet is marked for deletion, we do nothing and we don't requeue the request. It will be GCed.
		return reconcile.Result{}, nil
	}

	expectedReplicas := instance.Spec.Replicas
	currentPodList, err := r.listAllPodsWithLabel(instance)

	if err != nil {
		reqLogger.Error(err, "Failed to list pods with label due to error")
		return reconcile.Result{}, err
	}

	reqLogger.Info("Compare pod numbers", "current", len(currentPodList.Items), "expected", expectedReplicas)
	diff := len(currentPodList.Items) - expectedReplicas
	if diff < 0 {
		diff *= -1
		for i := 0; i < diff; i++ {
			// Define a new Pod object
			pod := newPodForCR(instance)
			// Set PodSet instance as the owner and controller
			if err := controllerutil.SetControllerReference(instance, pod, r.scheme); err != nil {
				reqLogger.Error(err, "Failed to set pod owner", "Pod.Name", pod.Name)
				return reconcile.Result{}, err
			}
			reqLogger.Info("Creating a new Pod", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
			err = r.client.Create(context.TODO(), pod)
			if err != nil {
				reqLogger.Error(err, "Failed to create pod", "Pod.Name", pod.Name)
				return reconcile.Result{}, err
			}
		}
	} else if diff > 0 {
		for i := 0; i < diff; i++ {
			index := len(currentPodList.Items) - 1 - i
			pod := currentPodList.Items[index]
			reqLogger.Info("Deleting a Pod", "Pod.Namespace", pod.Namespace, "Pod.Name", pod.Name)
			err = r.client.Delete(context.TODO(), &pod)
			if err != nil {
				reqLogger.Error(err, "Failed to delete pod", "Pod.Name", pod.Name)
				return reconcile.Result{}, err
			}
		}
	}

	status := getNewStatus(currentPodList.Items)
	reqLogger.Info("New status", "status", status)
	_, err = r.updatePodSetStatus(instance, *status)
	if err != nil {
		reqLogger.Error(err, "Failed to update podset status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *ReconcilePodSet) listAllPodsWithLabel(cr *appv1alpha1.PodSet) (*corev1.PodList, error) {
	podList := &corev1.PodList{}

	opts := &client.ListOptions{}
	opts.SetLabelSelector(fmt.Sprintf("%s=%s", POD_LABEL_NAME, cr.Name))
	err := r.client.List(context.TODO(), opts, podList)
	if err != nil {
		return nil, err
	}
	return podList, nil
}

func getNewStatus(pods []corev1.Pod) *appv1alpha1.PodSetStatus {
	names := make([]string, 0)
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return &appv1alpha1.PodSetStatus{
		PodNames: names,
	}
}

func (r *ReconcilePodSet) updatePodSetStatus(cr *appv1alpha1.PodSet, newStatus appv1alpha1.PodSetStatus) (*appv1alpha1.PodSet, error) {
	oldStatus := cr.Status
	if reflect.DeepEqual(oldStatus, newStatus) {
		//the status already matches, nothing to do
		return cr, nil
	}
	cr.Status = newStatus
	err := r.client.Update(context.TODO(), cr)
	if err != nil {
		return nil, err
	}
	return cr, nil
}

// newPodForCR returns a busybox pod with the same name/namespace as the cr
func newPodForCR(cr *appv1alpha1.PodSet) *corev1.Pod {
	labels := map[string]string{
		POD_LABEL_NAME : cr.Name,
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cr.Name + "-pod-",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "busybox",
					Image:   "busybox",
					Command: []string{"sleep", "3600"},
				},
			},
		},
	}
}
