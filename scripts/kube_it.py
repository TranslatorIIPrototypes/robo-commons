import yaml 
import argparse
import os , sys
from functools import reduce
import platform

###
#
#  Steps Performed: 
#   1. Fill in the docker-compose file with the env variables it needs via `docker-compose config` tool
#   2. Use this intermidiate file generated and tweak bad service names 
#   3. Also use this intermidiate file to generate the Persistance volumes(PVs) we need and associate claims (PVCs) for them
#   4. Generate a kubernetes file that contains all things we can get from a docker file
#   5. Tweak the final kubernetes generated file and treat the auto generated PVs and PVCs, replace them 
#   with what we just made. 
###

def get_pv_configs(tmp_file, out_dir, root_dir):
    # for this config map we are going to follow the pattern
    # given a volume mount on the docker config 
    #   ....
    #   volumes:
    #       - host_path/directory:/location/in/container
    # we are going to take `directory` as our id for traking down things and at times we might
    # need to use it in conjuction with host-path to test if we are refering to the same dir from multpile docker files .... but not sure yet.

    #make a Persistence volume sharable with multiple containers
    print('Creating volume claims')
    with open(tmp_file) as docker_file:
        docker_config = yaml.load(docker_file, Loader= yaml.FullLoader)

    server_address = platform.node() # get the host name// change this if you'd like to deploy into a different host. 

    # make a Persistence volume private to a single container
    make_pv = lambda name, storage_size, dir_name, private = True: \
        {   'apiVersion': 'v1',
            'kind': 'PersistentVolume',
            'metadata':{
                'name': name
            },
            'spec': {
                'capacity': {
                    'storage': storage_size
                },
                'accessModes': ['ReadWriteOnce'] if private == True else ['ReadWriteMany'] ,
                'persistentVolumeReclaimPolicy': 'Retain',
                'storageClassName': name,
                'nfs': {
                    'server': server_address,
                    'path': f'{dir_name}'
                }
            }
        }
    make_pv_claim = lambda name, storage_size, pv_name, private=True:\
        {
            'apiVersion': 'v1',
            'kind': 'PersistentVolumeClaim',
            'metadata': {
                'name': name
            },
            'spec': {
                'accessModes': ['ReadWriteOnce'] if private == True else ['ReadWriteMany'] ,
                'resources': {
                    'requests': {
                        'storage': storage_size,
                    },
                },
                'storageClassName': pv_name,                
            }
        }
    # given a docker volume like /path/to/host:/path/in/container:rw extract the /path/in/container part to use it for the Persisant volume claim 
    get_mount_point  = lambda docker_volume_config: docker_volume_config.split(':')[-2]
    # the would get the path on the host
    get_host_dir = lambda docker_volume_config: ':'.join(docker_volume_config.split(':')[:-2])
    shared_dirs = [ # assume everything else is one per container
            'logs',
            'shared'
        ]
    pv_configs_for_services = { #These configs should refer to the host dir
        'cache': {
            'size': '200Gi'
        },
        'logs': {
            'size': '10Gi',           
        },
        'neo4j_data': {
            'size': '300Gi'
        },        
        'neo4j_logs': {
            'size': '1Gi'
        },
        'neo4j_ssl': {
            'size': '20Ki'
        },
        'omnicorp_postgres' : {
            'size': '200Gi'
        },
        'robokop': {
            'size': '1Gi',            
        },
        'robokop-interfaces': { #the other dirs for robokop-interfaces are irrelvant to cluster deployment as we don't intend to do crawls there
            'size': '1Gi'
        },
        'robokop-messenger': {
            'size': '1Gi'
        },
        'robokop-rank': {
            'size': '1Gi'
        },
        'shared': {
            'size': '2Mi',            
        }
    }
    services = docker_config['services']
    mount_points = {} 
    for srvc in services:
        volume_configs = {'apiVersion': 'v1', 'kind': 'List', 'items': [] }# we will collect our PV and PVCs here and dump them
        service = services[srvc]
        container_name = service['container_name'] if 'container_name' in service else srvc        
        mount_points[srvc] = []
        if not 'volumes' in service:
            continue
        print(f'Found volume declarations for docker-service: {srvc}')
        for volume in  service['volumes']:
            host_path = get_host_dir(volume)
            # mount path is needed by the container created by kubernets so we will attach it to the service name and send it 
            # we also need the claim name to go along with it 
            # check out https://kubernetes.io/docs/concepts/storage/persistent-volumes/#claims-as-volumes
            mount_path = get_mount_point(volume)
            # lets get the final dir from the host dir to use it on out pv_configs map
            _dir = (host_path.split('\\') if '\\' in host_path else host_path.split('/'))[-1] #windows path vs linux we don't really need it but... 
            # If we haven't defined a dir in the map just skip making things around it
            if _dir not in pv_configs_for_services:
                continue
            pv_config = pv_configs_for_services[_dir]
            _dir_fixed = _dir.replace('_','-')
            pv_name = f'robokop-{_dir_fixed}-pv'
            pvc_name = f'robokop-{_dir_fixed}-pvc' 
            is_shared = _dir in shared_dirs
            #the following are the ones that we'd like to write            
            pvc_config_instance = make_pv_claim(pvc_name, pv_config['size'],pv_name,)
            pv_config_instance = make_pv(pv_name, pv_config['size'],host_path, private= not is_shared)

            print('**************************************')
            print(host_path)
            print(pv_name)            
            print('**************************************')
            if is_shared:
                # we would want to define the claims shared among services once     
                shared_things = {'apiVersion': 'v1', 'kind': 'List', 'items': [] }     
                shared_things['items'].append(pv_config_instance)
                shared_things['items'].append(pvc_config_instance)
                with open(f'{out_dir}/{pv_name}.yml','w') as pv_file:
                    yaml.dump(shared_things, pv_file)  # dump the PV and PVC for the shared things together
            else:
                volume_configs['items'].append(pv_config_instance)
                volume_configs['items'].append(pvc_config_instance)
            mount_points[srvc].append( # these are going to be used to modify our final kuberenetes generated file, specifically the volume mounts. 
                {
                    'name': f'{_dir_fixed}-volume',
                    'persistenceVolumeClaim': pvc_config_instance['metadata']['name'],
                    'mountPath': mount_path,
                    'container_name': container_name
                })
            print(f'\t {_dir}')
        with open(f'{out_dir}/robokop-{srvc}-persitence.yml', 'w') as pv_config_file:
            yaml.dump(volume_configs, pv_config_file)
    return mount_points


def tweak_deployment_kube_config(item, mount_points):
    """
    Deployment config, this function would change.
    """
    print(f"\tChanging config for deployment: `{item['metadata']['name']}`")
    service_name = item['metadata']['name']
    if service_name in mount_points:
        service_mounts = mount_points[service_name]
        containers = item['spec']['template']['spec']['containers']
        for container in containers:
            volumeMounts = []            
            for mount in service_mounts: 
                # incase the container name is different from the service name in docker this becomes an issue.
                container_name = mount['container_name']
                if container['name'] == container_name:                
                    volumeMounts.append({
                        'name': mount['name'],
                        'mountPath': mount['mountPath']
                    })
            container['volumeMounts'] = volumeMounts
        item['spec']['template']['spec']['volumes'] = list(map(lambda mount: {
            'name': mount['name'],
            'persistentVolumeClaim':{
                'claimName': mount['persistenceVolumeClaim']
            }
        }, mount_points[service_name]))
    return item


def tweak_service_kube_config(item):
    # We are deploying this to a specific machine so we don't really care 
    # about load balancer 
    # so we will make the port a node port 
    # and remove the load_balancer feature
    print(f"\tChanging config for service: `{item['metadata']['name']}`")
    #this is to avoid changing the type to node port when ports are not actually there for the container,
    contains_headless = reduce(lambda x, y : x or y['targetPort'] == 0, item['spec']['ports'], False)
    if not contains_headless:
        item['spec']['type'] = 'NodePort'
    if 'status' in item :
        del item['status']
    return item


def tweak_kube_files(out_file, mount_points):
    """ Adjustments for the kubernetes file.
    """
    print(f'Tweaking Kube-file: {out_file}')
    with open(out_file) as kube_file:
        kube_config = yaml.load(kube_file, Loader=yaml.FullLoader)
    items = kube_config['items']
    for item in items:
        if item['kind'] == 'Service':        
            tweak_service_kube_config(item)
        if item['kind'] == 'Deployment':
            tweak_deployment_kube_config(item, mount_points)
    # filter out pv claims since we don't like the default generated
    items = list(filter(lambda x: x['kind'] != 'PersistentVolumeClaim',items))
    kube_config['items'] = items
    with open(out_file, 'w') as kube_file:
        yaml.dump(kube_config, kube_file)


def run_command(cmd):
    """ Runs command.
    """
    try:
        os.system(cmd)
    except Exception as e: 
        print(f'Error running command {cmd}')
        print(e)
        exit()


def make_kube_files(docker_compose_config, out_put_file):
    """ Creates the kube config based off of a docker compose file.
    """
    command = f'kompose convert -f {docker_compose_config} -o {out_put_file}'
    run_command(command)    


def docker_config(docker_compose_file, file_name, env_file = ''):
    """ Creates docker-compose file with values filled out of the env_file. 
    """
    if env_file:
        with open(env_file) as file:
            for line in file.readlines():                
                line = line.strip()                
                if line.startswith('#') or line == None or line == '':
                    continue
                else:
                    key, value = line.split('=')
                    os.environ[key] = value
    command = f'docker-compose -f {docker_compose_file} config > {file_name}'
    run_command(command)


def convert_single_compose_file(in_file, out_file, root, tmp_file = '~tmp.yml' ,env_file = '', out_dir = ''):
    """ Converts a single docker-compose file to a self contained kubernets.
    """
    print(f'Filling configs {in_file} and saving to {tmp_file}')
    docker_config(in_file, tmp_file, env_file)
    docker_config_tweaking(tmp_file)
    if out_dir == '':
        out_dir = f"{'/'.join(':'.join(out_file.split(':')[-2:]).split('/')[:-1])}/"
    # config_persisent_volumes(tmp_file, out_dir)
    mount_points = get_pv_configs(tmp_file, out_dir, root)
    print(f'Making kube file {out_file}')
    #make kube files
    make_kube_files(tmp_file,out_file)
    #we can remove our temporary config file
    print(f'Removing temporary file {tmp_file}')
    run_command(f'rm -f {tmp_file}')
    #tweak kube files
    tweak_kube_files(out_file, mount_points)
    print('Done')


def correct_service_names(config):
    """
    Normalizes service names of docker containers so they turn out fine on kubernetes.
    """
    services = config['services']
    for service in services:
        service_new_name = service
        if '_' in service:
            service_new_name = ''.join(service.split('_'))
        if 'container_name' in services[service]:
            container_name = services[service]['container_name']
            services[service]['container_name'] = ''.join(container_name.split('_'))
        if service_new_name != service:
            services[service_new_name] = services[service]
            del services[service]
    return config


def correct_images (config):
    """
    Corrects docker images names for local builds.
    """
    # this is a step to convert some of the built images to point docker hub ones
    services = config['services']
    # Images to service mapping. Map out pre built images out in a docker repo. 
    #@TODO these are to change to a more central location.
    image_maps = {
        'knowledgegraph': 'yaphetkg/knowledgegraph',
        'manager': 'yaphetkg/manager',
        'messenger': 'yaphetkg/messenger',
        'rank': 'yaphetkg/robokop_rank',
        'interfaces': 'yaphetkg/robokop_builder'
    }
    for service in services:    
        if service in image_maps:
            services[service]['image'] = image_maps[service]
    return config


def docker_config_tweaking(docker_config_file):
    """ Applies some configs.
    """
    with open(docker_config_file) as tmp_f:
        docker_cnf = yaml.load(tmp_f, Loader= yaml.FullLoader)    
    # Make sure to normalize service names to something kubernets.
    docker_cnf = correct_service_names(docker_cnf)
    # Make sure that the images are in a docker repository. 
    docker_cnf = correct_images(docker_cnf)
    with open(docker_config_file,'w') as out:
        yaml.dump(docker_cnf, out)


def config_persisent_volumes(docker_config_file, out_dir):
    
    with open(docker_config_file) as docker_file:
        docker_config = yaml.load(docker_file, Loader= yaml.FullLoader)
    services = docker_config['services']
    for service in services:        
        configs = {'apiVersion': 'v1', 'items': []}
        srvc = services[service]
        if not 'volumes' in srvc:
            continue
        volumes = srvc['volumes']
        print(f'found service {service} with volumes {volumes}')
        for index, volume in enumerate(volumes):
            volume_path = ':'.join(volume.split(':')[:-2])
            volume_name = f'{service}-{index}'
            config = {
                'apiVersion': 'v1',
                'kind': 'PersistentVolume',
                'metadata': {
                    'name': volume_name
                },
                'spec': {
                    'capacity': {
                        'storage': '1Gi'
                    },
                    'volumeMode': 'Filesystem',
                    'accessModes': [
                        'ReadWriteOnce'
                    ],
                    'persistentVolumeReclaimPolicy': 'Recycle',
                    'storageClassName': volume_name,
                    'nfs': {
                        'path': volume_path,
                        'server': 'arrival.edc.renci.org' #maybe resolve this automagically @_@
                    }
                }
            }
            configs['items'].append(config)
        if len(configs['items']) > 0:
            
            pv_file_name = f'{out_dir}{service}-pv.yml'
            with open(pv_file_name, 'w') as out_file:
                print(f'writing out PV definition to : {pv_file_name}')
                yaml.dump(configs, out_file)


def convert_every_one(robokop_root, out_dir,tmp_file):
    # converts multiple 
    ## lets have a map of where our docker-compose.yml files are
    ##
    ## Add new docker compose files here. 
    ##
    docker_yml_map = {
        #manager things ---begin
        'backend': 'robokop/deploy/backend/',
        'manager': 'robokop/deploy/manager/',
        #'proxy': 'robokop/deploy/proxy', #we don't really want to deploy this now I think
        # manager things ---end
        #interfaces --- begin
        'cache': 'robokop-interfaces/deploy/cache/',
        'knowledgegraph':'robokop-interfaces/deploy/graph/',
        'interfaces': 'robokop-interfaces/deploy/',
        #interfaces --- end
        #messenger --- being
        'messenger': 'robokop-messenger/',
        #messenger --- end
        #ranker --begin
        'ranker': 'robokop-rank/deploy/ranker/',
        'omnicorp': 'robokop-rank/deploy/omnicorp/',
        # ranker ---end
    }

    # now lets try to make a dir 
    print(f'Creating output dir {out_dir}')
    run_command(f'mkdir -p {out_dir}')
    for services in docker_yml_map:
        print(f'Converting {services}')
        docker_yaml_full_path = f"{robokop_root}/{docker_yml_map[services]}docker-compose.yml"
        print(f"Docker yaml path : {docker_yaml_full_path}")
        kube_file_name = f'{out_dir}/{services}-kube-conf.yml'
        print(f'Creating Kubernets config : {kube_file_name}')
        convert_single_compose_file(docker_yaml_full_path, kube_file_name, env_file= f'{robokop_root}/shared/robokop.env', tmp_file= tmp_file, out_dir= out_dir, root= robokop_root)



if __name__ == "__main__":    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--in-file', help="Input docker-compose file path")
    parser.add_argument('-o','--out-file', help= "Output Kubernetes config file path, should be run together with -i")
    parser.add_argument('-r','--root', help="Root dir of the project." )
    parser.add_argument('-O', '--output-dir', help="Output dir to put all the kubernets files. Should be run together with -r.")
    #populate config values
    tmp_file = '~tmp.yml'
    in_file = parser.parse_args().in_file
    out_file = parser.parse_args().out_file
    root = parser.parse_args().root
    out_dir = parser.parse_args().output_dir
    print(out_dir)
    if not root:
        print('Please provide -r (root dir for the project)')
    if in_file != None and out_file != None:
        convert_single_compose_file(in_file, out_file, root)
        print('converted file exiting')
        exit()
    elif out_dir:
        convert_every_one(root, out_dir, f'{root}/{tmp_file}')
        exit()
    else :
        print('Error invalid usage, you can use -i with -o and -r with -O together as pairs.')
        exit()
    


