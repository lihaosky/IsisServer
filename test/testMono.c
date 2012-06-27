#include <mono/jit/jit.h>
#include <mono/metadata/object.h>
#include <mono/metadata/environment.h>
#include <mono/metadata/assembly.h>
#include <mono/metadata/debug-helpers.h>
#include <string.h>
#include <stdlib.h>

#ifndef false 
#define false 0
#endif

#ifndef true
#define true 1
#endif

int main(int argc, char **argv) {
	MonoDomain *domain;
	MonoAssembly *assembly;
	MonoImage *image;
	MonoClass *class;
	MonoMethodDesc *mdesc;
	MonoMethod *mmethod, *m;
	const char *file;
	void *args[3];
	int nodeNum, shardSize, myRank;
	void *iter;
	
	nodeNum = 1;
	shardSize = 1;
	myRank = 0;
	
	if (argc < 2) {
		fprintf(stderr, "Provide an assembly to load!\n");
		return 1;
	}
	
	file = argv[1];
	domain = mono_jit_init(file);
	assembly = mono_domain_assembly_open(domain, file);
	if (!assembly) {
		fprintf(stderr, "Fail to open assembly!\n");
		return 1;
	}
	
	mono_jit_exec(domain, assembly, argc - 1, argv + 1);
	image = mono_assembly_get_image(assembly);
	class = mono_class_from_name(image, "IsisService", "IsisServer");
	
	while ((m = mono_class_get_methods(class, &iter))) {
		if (strcmp(mono_method_get_name(m), "createGroup") == 0) {
			mmethod = m;
			break;
		}
	}
	
	args[0] = &nodeNum;
	args[1] = &shardSize;
	args[2] = &myRank;
	
	mono_runtime_invoke(mmethod, NULL, args, NULL);
	
	printf("Here\n");	
}
