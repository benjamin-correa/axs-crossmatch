# AXS-CROSSMATCH  <!-- omit in toc -->

- [Convención de Git commits](#convención-de-git-commits)
- [Reglas y lineamientos](#reglas-y-lineamientos)
- [Instalar dependencias](#instalar-dependencias)

# Convención de Git commits
   - [build]: Cambios relacionados con la construcción (por ejemplo: añadir dependencias externas, cambio en el flujo de trabajo de CI)
   - [chore]: Un cambio en el código que el usuario externo no verá (por ejemplo: cambio en el archivo .gitignore o en el archivo .prettierrc)
   - [refactor]: Un código que no corrige un error ni añade una característica. (ej.: Puedes usar esto cuando hay cambios semánticos como renombrar el nombre de una variable/función)
   - [feat]: Una nueva característica
   - [fix]: Una corrección de errores
   - [docs]: Cambios relacionados con la documentación
   - [perf]: Un código que mejora el rendimiento
   - [style]: Un código que está relacionado con el estilo
   - [test]: Añadir una nueva prueba o hacer cambios en una prueba existente

# Reglas y lineamientos
Para obtener lo mejor de estructura de código:

   - No eliminar ninguna línea del archivo .gitignore que se proporciona
   - No subir datos al repositorio
   - No dejar credenciales en el repositorio

# Instalar dependencias
Declara cualquier dependencia en `src/requirements.txt` para la instalación de `pip`

Para instalarlos, ejecuta:
```
pip install -r src/requirements.txt
```
