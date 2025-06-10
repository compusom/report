# Report

Generador de Reportes en formato de texto para Bitácora y Rendimiento.

## Requisitos
- Python 3.11 o superior
- Dependencias: `pandas`, `numpy`, `python-dateutil` y soporte para `tkinter`.

Instala las dependencias con:
```bash
pip install pandas numpy python-dateutil
```

## Uso
Ejecuta la interfaz gráfica con:
```bash
python main.py
```

Desde la ventana podrás seleccionar los archivos de datos y generar reportes de **Bitácora**, **Rendimiento** o un resumen semanal listo para copiar en **Notion**.
Los reportes se imprimen en la consola en un formato que puede copiarse a Notion.

## Pruebas
Este proyecto usa `pytest` para las pruebas unitarias. Ejecuta todas las pruebas con:
```bash
pytest
```

