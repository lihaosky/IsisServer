#include <mono/jit/jit.h>
#include <mono/metadata/object.h>
#include <mono/metadata/environment.h>
#include <mono/metadata/assembly.h>
#include <mono/metadata/debug-helpers.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#ifndef false 
#define false 0
#endif

#ifndef true
#define true 1
#endif

int agc;
char **agv;
int nodeNum = 1;
int shardSize = 1;
int myRank = 0;
MonoDomain *domain;
MonoAssembly *assembly;
MonoImage *image;
MonoClass *class;
const char *file;
void *iter;

void* isis_start() {
	MonoMethod *m, *mmethod;
	void *args[3];
	
	domain = mono_jit_init(file);
	mono_config_parse(NULL);
	assembly = mono_domain_assembly_open(domain, file);
	if (!assembly) {
		fprintf(stderr, "Fail to open assembly!\n");
		exit;
	}
	mono_jit_exec(domain, assembly, agc - 1, agv + 1);
	image = mono_assembly_get_image(assembly);
	class = mono_class_from_name(image, "IsisService", "IsisServer");
	
	while ((m = mono_class_get_methods(class, &iter))) {
	    	printf("Method %s\n", mono_method_get_name(m));

		if (strcmp(mono_method_get_name(m), "createGroup") == 0) {
			mmethod = m;
			break;
		}
	}
	
	args[0] = &nodeNum;
	args[1] = &shardSize;
	args[2] = &myRank;
	
	//mono_runtime_invoke(mmethod, NULL, args, NULL);
}

void safe_send(char *command, int rank) {
	MonoMethod *m, *send_method;
	MonoString *cmd;
	void *args[2];
	
	while ((m = mono_class_get_methods(class, &iter))) {
		printf("Method %s\n", mono_method_get_name(m));
		
		if (strcmp(mono_method_get_name(m), "commandSend") == 0) {
			send_method = m;
			break;
		}
	}
	
	cmd = mono_string_new(domain, command);
	args[0] = cmd;
	args[1] = &rank;
	
	mono_runtime_invoke(send_method, NULL, args, NULL);
}

int main(int argc, char **argv) {
	pthread_t thread;
	
	if (argc < 2) {
		fprintf(stderr, "Provide an assembly to load!\n");
		return 1;
	}
	
	file = argv[1];
	agc = argc;
	agv = argv;
	pthread_create(&thread, NULL, isis_start, NULL);
	sleep(20);
	safe_send("insert li", 10);
	printf("Here\n");	
}
