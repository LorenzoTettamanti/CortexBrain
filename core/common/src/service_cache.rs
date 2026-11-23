use anyhow::Error;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ObjectList;
use kube::{Api, Client};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::str::FromStr;
use tracing::debug;

#[derive(Clone, Debug, Default)]
pub struct ServiceCache {
    // service_map should maitain the Option Type. Since Option type can be also None
    // it's used to deal all the cases
    pub service_map: Option<HashMap<String, Vec<String>>>,
    pub pod_ip_map: Option<HashMap<Ipv4Addr, Vec<String>>>,
}
impl ServiceCache {
    pub fn init(&mut self) {
        self.service_map = Some(HashMap::<String, Vec<String>>::new());
        self.pod_ip_map = Some(HashMap::<Ipv4Addr, Vec<String>>::new());
    }

    /// Helper function to query all pods form the Kubernetes API
    pub async fn query_all_pods_from_kubeapi(&mut self) -> Result<ObjectList<Pod>, Error> {
        debug!("Connecting to kubernetes client");
        let client = Client::try_default().await?;
        debug!("Querying all the pods");
        let pods: Api<Pod> = Api::all(client);
        let lp = kube::api::ListParams::default(); // default list params
        let pod_list = pods.list(&lp).await?;

        Ok(pod_list)
    }

    /// Helper function to populate and mantain a cache service map
    pub async fn populate_map_with_pod_info(&mut self) -> Result<(), Error> {
        if Client::try_default().await.is_ok() {
            let all_pods = self.query_all_pods_from_kubeapi().await?;

            debug!("Querying and updating all the pods");
            if let Some(map) = self.service_map.as_mut() {
                map.clear();
            }
            if let Some(map) = self.pod_ip_map.as_mut() {
                map.clear();
            }
            for pod in all_pods {
                let (Some(name), Some(uid), Some(namespace)) = (
                    pod.metadata.name.clone(),
                    pod.metadata.uid.clone(),
                    pod.metadata.namespace.clone(),
                ) else {
                    continue;
                };
                let annotations = pod.metadata.annotations.clone().unwrap_or_default();

                // docs from kubernetes
                // Annotations is an unstructured key value map stored with a resource that
                // may be set by external tools to store and retrieve arbitrary metadata.
                // They are not queryable and should be preserved when modifying objects.
                //More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations

                if let Some(map) = self.service_map.as_mut() {
                    // static pods are managed by the kubelet itself instead all the other pods
                    // are managed by controllers without the need of the API server
                    // the kubelet mirrors the static pods but the UID does not match the real signature of the pod (verified)
                    // so the algorithm check if the pod is static by checking if the annotations contains the word 'file' or 'http'
                    // reference for the labels: https://kubernetes.io/docs/reference/labels-annotations-taints/
                    // and copies the config.hash (aka the signature of the static pod) into the service cache instead of the UID

                    // more about static pods: https://medium.com/@iamsteffinissac/mastering-static-pods-in-kubernetes-bootstrapping-troubleshooting-mirror-pods-advanced-update-a35a5f95e329

                    let is_static_pod = annotations
                        .get("kubernetes.io/config.source")
                        .is_some_and(|a| a == "file" || a == "http");

                    let key = if is_static_pod {
                        annotations
                            .get("kubernetes.io/config.hash")
                            .cloned()
                            .unwrap_or(uid)
                    } else {
                        uid
                    };
                    map.insert(key, vec![name.clone(), namespace.clone()]);
                }

                if let Some(map) = self.pod_ip_map.as_mut() {
                    let mut pod_ips = Vec::new();
                    if let Some(status) = &pod.status {
                        if let Some(ip) = &status.pod_ip {
                            pod_ips.push(ip.clone());
                        }
                        if let Some(ips) = &status.pod_ips {
                            pod_ips.extend(ips.iter().map(|pod_ip| pod_ip.ip.clone()));
                        }
                    }

                    for ip in pod_ips {
                        if let std::result::Result::Ok(ip) = Ipv4Addr::from_str(ip.trim()) {
                            map.insert(ip, vec![name.clone(), namespace.clone()]);
                        }
                    }
                }
            } // insert the pod name and uid from the KubeAPI
        } else {
            debug!("Kubernetes environment not available");
        }

        Ok(())
    }
    pub fn lookup_ip(&self, ip: &Ipv4Addr) -> Option<&Vec<String>> {
        self.pod_ip_map.as_ref().and_then(|map| map.get(ip))
    }

    pub async fn get_from_cache(&self, container_id: &str) -> Option<&Vec<String>> {
        // pass a container_id and returns the object meta infos:
        /* pub struct ObjectMeta {
            pub annotations: Option<BTreeMap<String, String>>,
            pub creation_timestamp: Option<Time>,
            pub deletion_grace_period_seconds: Option<i64>,
            pub deletion_timestamp: Option<Time>,
            pub finalizers: Option<Vec<String>>,
            pub generate_name: Option<String>,
            pub generation: Option<i64>,
            pub labels: Option<BTreeMap<String, String>>,
            pub managed_fields: Option<Vec<ManagedFieldsEntry>>,
            pub name: Option<String>,
            pub namespace: Option<String>,
            pub owner_references: Option<Vec<OwnerReference>>,
            pub resource_version: Option<String>,
            pub self_link: Option<String>,
            pub uid: Option<String>,
        } */

        if let Some(map) = self.service_map.as_ref() {
            // cache hit that contains ["name","namespace"] or None
            if let Some(cached_metadata) = map.get(container_id) {
                Some(cached_metadata)
            } else {
                None
            }
        } else {
            None
        }
    }
}
