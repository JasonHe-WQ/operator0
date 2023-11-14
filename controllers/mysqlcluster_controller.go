/*
Copyright 2023.

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

package controllers

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1 "github.com/example/mysql-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MySQLClusterReconciler reconciles a MySQLCluster object
type MySQLClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=mysql.example.com,resources=mysqlclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mysql.example.com,resources=mysqlclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mysql.example.com,resources=mysqlclusters/finalizers,verbs=update

func (r *MySQLClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	cluster := &mysqlv1.MySQLCluster{}
	err := r.Get(ctx, req.NamespacedName, cluster)
	if err != nil {
		if errors.IsNotFound(err) {
			// 对象已被删除，不再处理
			return ctrl.Result{}, nil
		}
		// 读取对象时发生错误，返回错误
		return ctrl.Result{}, err
	}

	// 1. 创建或更新Secret
	err = r.reconcileSecret(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 2. 创建或更新ConfigMap
	err = r.reconcileConfigMap(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. 创建或更新StatefulSet
	err = r.reconcileStatefulSet(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. 创建或更新Headless Service
	err = r.reconcileHeadlessService(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. 创建或更新Read Service
	err = r.reconcileReadService(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MySQLClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1.MySQLCluster{}).
		Complete(r)
}

func (r *MySQLClusterReconciler) reconcileSecret(ctx context.Context, cluster *mysqlv1.MySQLCluster) error {
	secret := &corev1.Secret{}
	secretName := fmt.Sprintf("%s-secret", cluster.Name)
	secretNamespace := cluster.Namespace

	// 检查Secret是否已存在
	err := r.Get(ctx, client.ObjectKey{Name: secretName, Namespace: secretNamespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		// 处理读取Secret时的错误
		return err
	}

	// Secret不存在时创建新的Secret
	if errors.IsNotFound(err) {
		newSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: secretNamespace,
			},
			StringData: map[string]string{
				"username": cluster.Spec.UserName,
				"password": cluster.Spec.Password,
			},
		}

		err := r.Create(ctx, newSecret)
		if err != nil {
			// 处理创建Secret时的错误
			return err
		}
	} else {
		// 更新已存在的Secret
		secret.StringData = map[string]string{
			"username": cluster.Spec.UserName,
			"password": cluster.Spec.Password,
		}

		err := r.Update(ctx, secret)
		if err != nil {
			// 处理更新Secret时的错误
			return err
		}
	}

	return nil
}
func int32Ptr(i int32) *int32 { return &i }
func (r *MySQLClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *mysqlv1.MySQLCluster) error {
	size := cluster.Spec.Size
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(size),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "mysql"},
			},
			// ...

			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{

					InitContainers: []corev1.Container{
						{
							Name:  "init-mysql",
							Image: "mysql:8.0",
							Command: []string{
								"bash",
								"-c",
								`set -ex
                             [[ $(hostname) =~ -([0-9]+)$ ]] || exit 1
                             ordinal=${BASH_REMATCH[1]}
                             echo [mysqld] > /mnt/conf.d/server-id.cnf
                             echo server-id=$((100 + $ordinal)) >> /mnt/conf.d/server-id.cnf

							# 计算buffer大小，假设资源请求的内存值是以Mi或Gi结尾
             				memory_in_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
             				memory_in_megabytes=$((memory_in_bytes / 1024 / 1024))
             				buffer_size=$((memory_in_megabytes / 2))M

             				echo "innodb_buffer_pool_size=$buffer_size" >> /mnt/conf.d/custom.cnf
                             if [[ $ordinal -eq 0 ]]; then
                               cp /mnt/config-map/master.cnf /mnt/conf.d/
                             else
                               cp /mnt/config-map/slave.cnf /mnt/conf.d/
                             fi`,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "config",
									MountPath: "/mnt/conf.d",
								},
								{
									Name:      "config-map",
									MountPath: "/mnt/config-map",
								},
							},
						},
						{
							Name:  "clone-mysql",
							Image: "gcr.io/google-samples/xtrabackup:1.0",
							Command: []string{
								"bash",
								"-c",

								`set -ex
								# 拷贝操作只需要在第一次启动时进行，所以如果数据已经存在，跳过
          						[[ -d /var/lib/mysql/mysql ]] && exit 0
          						# Master节点(序号为0)不需要做这个操作
          						    if hostname | grep -qE '-0$'; then
        								exit 0
    								fi
							  	ordinal=${BASH_REMATCH[1]}
							  	[[ $ordinal -eq 0 ]] && exit 0
							 	 # 使用ncat指令，远程地从前一个节点拷贝数据到本地
							  	ncat --recv-only mysql-$(($ordinal-1)).mysql 3307 | xbstream -x -C /var/lib/mysql
							  	# 执行--prepare，这样拷贝来的数据就可以用作恢复了
							 	 xtrabackup --prepare --target-dir=/var/lib/mysql`,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "mysql-data",
									MountPath: "/var/lib/mysql",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "mysql",
							Image: "mysql:8.0",
							// ...
							Ports: []corev1.ContainerPort{
								{
									Name:          "mysql",
									ContainerPort: 3306,
								},
							},
							Resources: cluster.Spec.Resources,
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"bash",
											"-c",
											"mysql -u $MYSQL_USER -p$MYSQL_PASSWORD -e 'CREATE DATABASE IF NOT EXISTS liveness; USE liveness; CREATE TABLE IF NOT EXISTS liveness (id INT PRIMARY KEY, ts TIMESTAMP) AUTO_INCREMENT = 1; INSERT INTO liveness (id, ts) VALUES (1, NOW()) ON DUPLICATE KEY UPDATE ts=NOW();'",
										},
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								FailureThreshold:    3,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"bash",
											"-c",
											"mysql -u $MYSQL_USER -p$MYSQL_PASSWORD -e 'UPDATE liveness.liveness SET ts=NOW() WHERE id=1;'",
										},
									},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       1,
								FailureThreshold:    1,
							},
							Env: []corev1.EnvVar{
								{
									Name: "MYSQL_ROOT_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: cluster.Name + "-secret",
											},
											Key: "password",
										},
									},
								},
								{
									Name: "MYSQL_USER",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: cluster.Name + "-secret",
											},
											Key: "username",
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "mysql-data",
									MountPath: "/var/lib/mysql",
								},
								{
									Name:      "config",
									MountPath: "/etc/mysql/conf.d",
								},
							},
						},
						{
							Name:  "xtrabackup",
							Image: "gcr.io/google-samples/xtrabackup:1.0",
							Ports: []corev1.ContainerPort{
								{
									Name:          "xtrabackup",
									ContainerPort: 3307,
								},
							},
							Command: []string{
								"bash",
								"-c",
								`set -ex
								cd /var/lib/mysql
							
								# 从备份信息文件里读取MASTER_LOG_FILEM和MASTER_LOG_POS这两个字段的值，用来拼装集群初始化SQL
								if [[ -f xtrabackup_slave_info ]]; then
								  mv xtrabackup_slave_info change_master_to.sql.in
								  rm -f xtrabackup_binlog_info
								elif [[ -f xtrabackup_binlog_info ]]; then
								  read -r master_log_file master_log_pos < <(cat xtrabackup_binlog_info)
								  rm xtrabackup_binlog_info
								  echo "CHANGE MASTER TO MASTER_LOG_FILE='$master_log_file', MASTER_LOG_POS=$master_log_pos" > change_master_to.sql.in
								fi
							
								# 如果change_master_to.sql.in，就意味着需要做集群初始化工作
								if [[ -f change_master_to.sql.in ]]; then
								  echo "Waiting for mysqld to be ready (accepting connections)"
								  until mysql -h 127.0.0.1 -e "SELECT 1"; do sleep 1; done
							
								  echo "Initializing replication from clone position"
											  mv change_master_to.sql.in change_master_to.sql.orig
											  mysql -h 127.0.0.1 <<EOF
											  $(<change_master_to.sql.orig),
												MASTER_HOST='mysql-0.mysql',
												MASTER_USER='root',
												MASTER_PASSWORD='',
												MASTER_CONNECT_RETRY=10;
											  START SLAVE;
											  EOF
									fi
										
								exec ncat --listen --keep-open --send-only --max-conns=1 3307 -c "xtrabackup --backup --slave-info --stream=xbstream --host=127.0.0.1 --user=root"`,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "mysql-data",
									MountPath: "/var/lib/mysql",
								},
								{
									Name:      "config",
									MountPath: "/etc/mysql/conf.d",
								},
							},
							// 配置Sidecar具体行为
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "mysql-data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: cluster.Name + "-pvc",
								},
							},
						},
						{
							Name: "config-map",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: cluster.Name + "-config",
									},
								},
							},
						},
					},
				},
			},
			// ...
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: cluster.Name + "-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("100Gi"),
							},
						},
					},
				},
			},
		},
	}

	// 创建或更新StatefulSet
	err := r.createOrUpdateStatefulSet(ctx, sts)
	if err != nil {
		// 处理错误
		return err
	}
	return nil
}
func (r *MySQLClusterReconciler) createOrUpdateStatefulSet(ctx context.Context, cluster *appsv1.StatefulSet) error {
	// ...
	// 检查StatefulSet是否已存在
	found := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace}, found)
	if err != nil && !errors.IsNotFound(err) {
		// 读取错误，返回错误
		return err
	}

	if errors.IsNotFound(err) {
		// StatefulSet不存在，创建新的StatefulSet
		err := r.Create(ctx, cluster)
		if err != nil {
			// 创建时错误，返回错误
			return err
		}
	} else {
		// StatefulSet已存在，更新现有的StatefulSet
		found.Spec = cluster.Spec
		err := r.Update(ctx, found)
		if err != nil {
			// 更新时错误，返回错误
			return err
		}
	}

	// StatefulSet创建或更新成功
	return nil
}

func (r *MySQLClusterReconciler) createOrUpdateService(ctx context.Context, svc *corev1.Service) error {
	foundSvc := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKey{Name: svc.Name, Namespace: svc.Namespace}, foundSvc)
	if err != nil && !errors.IsNotFound(err) {
		// 处理读取错误
		return err
	}

	if errors.IsNotFound(err) {
		// 服务不存在，创建新服务
		err = r.Create(ctx, svc)
		if err != nil {
			// 处理创建错误
			return err
		}
	} else {
		// 服务已存在，根据需要更新
		foundSvc.Spec.Ports = svc.Spec.Ports
		foundSvc.Spec.Selector = svc.Spec.Selector
		err = r.Update(ctx, foundSvc)
		if err != nil {
			// 处理更新错误
			return err
		}
	}
	return nil
}

func (r *MySQLClusterReconciler) reconcileReadService(ctx context.Context, cluster *mysqlv1.MySQLCluster) error {
	svcName := cluster.Name + "-read"
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": cluster.Name + "-mysql",
			},
			Ports: []corev1.ServicePort{
				{
					Name: "mysql",
					Port: 3306,
				},
			},
		},
	}

	// 创建或更新标准服务的逻辑
	err := r.createOrUpdateService(ctx, svc)
	if err != nil {
		// 处理错误
		return err
	}
	return nil
}
func (r *MySQLClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *mysqlv1.MySQLCluster) error {
	svcName := cluster.Name + "-write"
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				"app": cluster.Name + "-mysql",
			},
			Ports: []corev1.ServicePort{
				{
					Name: "mysql",
					Port: 3306,
				},
			},
		},
	}

	// 创建或更新无头服务的逻辑
	err := r.createOrUpdateService(ctx, svc)
	if err != nil {
		// 处理错误
		return err
	}
	return nil
}

func (r *MySQLClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *mysqlv1.MySQLCluster) error {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{
			"master.cnf": `
                # 主节点MySQL的配置文件
				[mysql]
				# mysql客户端默认字符集
				default-character-set=utf8
				[mysqld]
				# 数据库文件位置
				datadir=/var/lib/mysql
				# 允许最大连接数
				max_connections=1000
				# innodb的dml操作的行级锁的等待时间
				innodb_lock_wait_timeout=500
				# 设置mysql服务端默认字符集
				character-set-server=utf8mb4
				# 默认创建新数据的新建排序规则
				collation-server=utf8mb4_general_ci
				# 创建新表时将使用的默认存储引擎
				default-storage-engine=INNODB
				# 缓存大小
				sort_buffer_size=512MB
				# 大小写敏感配置项0为敏感，1为不敏感
				lower_case_table_names=1
				# 选择正8区
				default-time-zone='+8:00'
                log-bin`,
			"slave.cnf": `
                # 从节点MySQL的配置文件
				[mysql]
				# mysql客户端默认字符集
				default-character-set=utf8
				[mysqld]
				# 数据库文件位置
				datadir=/var/lib/mysql
				# 允许最大连接数
				max_connections=1000
				# innodb的dml操作的行级锁的等待时间
				innodb_lock_wait_timeout=500
				# 设置mysql服务端默认字符集
				character-set-server=utf8mb4
				# 默认创建新数据的新建排序规则
				collation-server=utf8mb4_general_ci
				# 创建新表时将使用的默认存储引擎
				default-storage-engine=INNODB
				# 缓存大小
				sort_buffer_size=512MB
				# 大小写敏感配置项0为敏感，1为不敏感
				lower_case_table_names=1
				# 选择正8区
				default-time-zone='+8:00'
                super-read-only`,
		},
	}

	// 检查ConfigMap是否已存在
	found := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKey{Name: configMap.Name, Namespace: configMap.Namespace}, found)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		err = r.Create(ctx, configMap)
		if err != nil {
			return err
		}
	} else {
		found.Data = configMap.Data
		err = r.Update(ctx, found)
		if err != nil {
			return err
		}
	}

	return nil
}
