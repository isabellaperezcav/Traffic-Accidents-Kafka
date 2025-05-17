import great_expectations as gx

# Inicializar el contexto
context = gx.get_context()

# Crear un directorio de configuración de Great Expectations
context.init_project()

print("Proyecto de Great Expectations inicializado correctamente.")
