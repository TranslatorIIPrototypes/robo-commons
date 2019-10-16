import yaml 
import argparse
import os , sys
from functools import reduce

def tweak_kube_files(out_file):
    """ Adjustments for the kubernetes file.
    """
    print(out_file)
    with open(out_file) as kube_file:
        kube_config = yaml.load(kube_file, Loader=yaml.FullLoader)
    for item in kube_config['items']:
        if item['kind'] == 'Service':        
            # We are deploying this to a specific machine so we don't really care 
            # about load balancer 
            # so we will make the port a node port 
            # and remove the load_balancer feature
            print(f"Changing config for service: `{item['metadata']['name']}`")
            #this is to avoid changing the type to node port when ports are not actually there for the container,
            contains_headless = reduce(lambda x, y : x or y['targetPort'] == 0, item['spec']['ports'], False)
            if not contains_headless:
                item['spec']['type'] = 'NodePort'
            if 'status' in item :
                del item['status']
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

def convert_single_compose_file(in_file, out_file, tmp_file = '~tmp.yml' ,env_file = ''):
    """ Converts a single docker-compose file to a self contained kubernets.
    """
    print(f'Filling configs {in_file} and saving to {tmp_file}')
    docker_config(in_file, tmp_file, env_file)
    docker_config_tweaking(tmp_file)
    print(f'Making kube file {out_file}')
    #make kube files
    make_kube_files(tmp_file,out_file)
    #we can remove our temporary config file
    print(f'Removing temporary file {tmp_file}')
    run_command(f'rm -f {tmp_file}')
    #tweak kube files 
    print(f'Tweaking {out_file}')
    tweak_kube_files(out_file)
    print('Done')

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
        convert_single_compose_file(docker_yaml_full_path, kube_file_name, env_file= f'{robokop_root}/shared/robokop.env', tmp_file= tmp_file)



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
    if in_file != None and out_file != None:
        convert_single_compose_file(in_file, out_file)
        print('converted file exiting')
        exit()
    elif root and out_dir:
        convert_every_one(root, out_dir, f'{root}/{tmp_file}')
        exit()
    else :
        print('Error invalid usage, you can use -i with -o and -r with -O together as pairs.')
        exit()
    


