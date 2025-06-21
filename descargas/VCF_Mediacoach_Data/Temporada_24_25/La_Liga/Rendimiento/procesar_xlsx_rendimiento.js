const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const parquet = require('parquetjs');

function extraerInfoPartido(texto) {
    try {
        // Dividir por |
        const partes = texto.split('|').map(parte => parte.trim());
        
        if (partes.length >= 4) {
            const liga = partes[0];
            const temporada = partes[1].replace('Temporada ', '');
            const jornada = partes[2];
            
            // Para el partido y fecha
            const partidoYFecha = partes[3];
            
            // Buscar fecha entre paréntesis
            const matchFecha = partidoYFecha.match(/\((\d{4}-\d{2}-\d{2})\)$/);
            let fecha = null;
            let partido = partidoYFecha;
            
            if (matchFecha) {
                fecha = matchFecha[1];
                partido = partidoYFecha.substring(0, matchFecha.index).trim();
            }
            
            return {
                liga,
                temporada,
                jornada,
                partido,
                fecha
            };
        }
        return null;
    } catch (error) {
        console.log(`Error extrayendo info del partido: ${error}`);
        return null;
    }
}

function extraerEquipo(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);

    for (let row = 0; row <= Math.min(30, range.e.r); row++) {
        for (let col = 0; col <= Math.min(20, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
            const cell = sheet[cellAddress];

            if (cell && typeof cell.v === 'string') {
                const valor = cell.v.trim().toLowerCase();

                if (valor.includes('informe de rendimiento físico')) {
                    const match = cell.v.match(/Informe de Rendimiento Físico\s+(.*)/i);
                    if (match && match[1]) {
                        const equipo = match[1].trim();
                        if (equipo.length > 0) {
                            return equipo;
                        }
                    }
                }
            }
        }
    }

    return null;
}


function encontrarInfoPartido(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);
    
    // Buscar en las primeras 15 filas
    for (let row = 0; row <= Math.min(15, range.e.r); row++) {
        for (let col = 0; col <= Math.min(15, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({r: row, c: col});
            const cell = sheet[cellAddress];
            
            if (cell && cell.v && typeof cell.v === 'string' && 
                cell.v.includes('|') && cell.v.includes('Temporada')) {
                return extraerInfoPartido(cell.v);
            }
        }
    }
    return null;
}

function encontrarFilaHeaders(sheet) {
    const range = XLSX.utils.decode_range(sheet['!ref']);
    
    // Buscar "Id Jugador" en las primeras 20 filas
    for (let row = 0; row <= Math.min(20, range.e.r); row++) {
        for (let col = 0; col <= Math.min(10, range.e.c); col++) {
            const cellAddress = XLSX.utils.encode_cell({r: row, c: col});
            const cell = sheet[cellAddress];
            
            if (cell && cell.v && String(cell.v).trim() === 'Id Jugador') {
                return row;
            }
        }
    }
    return null;
}

function procesarArchivoXlsx(archivoPath) {
    console.log(`Procesando: ${path.basename(archivoPath)}`);
    
    try {
        // Leer archivo
        const workbook = XLSX.readFile(archivoPath, {
            cellStyles: false,    // Ignorar estilos que pueden estar corruptos
            cellFormulas: true,   
            cellDates: true,     
            cellNF: false,        // Ignorar formato de números
            sheetStubs: true     
        });
        
        // Obtener primera hoja
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        
        // Encontrar información del partido
        const infoPartido = encontrarInfoPartido(sheet);
        if (!infoPartido) {
            console.log(`  ❌ No se encontró información del partido`);
            return null;
        }
        
        const equipo = extraerEquipo(sheet);
        if (!equipo) {
            console.log(`  ❌ No se encontró el equipo`);
            return null;
        }

        // Encontrar fila de headers
        const filaHeaders = encontrarFilaHeaders(sheet);
        if (filaHeaders === null) {
            console.log(`  ❌ No se encontró fila con 'Id Jugador'`);
            return null;
        }
        
        console.log(`  📍 Headers en fila: ${filaHeaders + 1}`);
        console.log(`  ⚽ ${infoPartido.jornada} | ${infoPartido.partido}`);
        
        // Convertir a JSON desde la fila de headers
        const jsonData = XLSX.utils.sheet_to_json(sheet, {
            range: filaHeaders, // Empezar desde la fila de headers
            header: 1
        });
        
        if (jsonData.length === 0) {
            console.log(`  ❌ No hay datos`);
            return null;
        }
        
        // Obtener headers y filtrar desde columna 2
        const headers = jsonData[0];
        const headersFiltered = headers.slice(1); // Desde columna 2
        
        // Procesar datos
        const datosProcessed = jsonData.slice(1).map(row => {
            const rowFiltered = row.slice(1); // Desde columna 2
            const obj = {};
            
            // Crear objeto con headers
            headersFiltered.forEach((header, index) => {
                obj[header || `columna_${index + 2}`] = rowFiltered[index];
            });
            
            // Agregar información del partido
            obj.equipo = equipo;
            obj.liga = infoPartido.liga;
            obj.temporada = infoPartido.temporada;
            obj.jornada = infoPartido.jornada;
            obj.partido = infoPartido.partido;
            obj.fecha = infoPartido.fecha;
            obj.archivo_origen = path.basename(archivoPath);
            
            return obj;
        }).filter(row => {
            // Filtrar filas vacías
            return Object.values(row).some(val => val !== null && val !== undefined && val !== '');
        });
        
        console.log(`  ✅ Procesadas ${datosProcessed.length} filas`);
        return datosProcessed;
        
    } catch (error) {
        console.log(`  ❌ Error: ${error.message}`);
        return null;
    }
}

async function escribirParquet(datos, nombreArchivo) {
    try {
        // Definir esquema dinámico basado en los datos
        const primerFilaDatos = datos[0];
        const schemaDef = {};

        Object.keys(primerFilaDatos).forEach(campo => {
            const valorEjemplo = primerFilaDatos[campo];
            let tipo = { type: 'UTF8' }; // Por defecto string

            if (typeof valorEjemplo === 'number') {
                if (Number.isInteger(valorEjemplo)) {
                    tipo = { type: 'INT64' };
                } else {
                    tipo = { type: 'DOUBLE' };
                }
            } else if (typeof valorEjemplo === 'boolean') {
                tipo = { type: 'BOOLEAN' };
            }

            schemaDef[campo] = tipo;
        });

        const schema = new parquet.ParquetSchema(schemaDef);

        // Crear writer
        const writer = await parquet.ParquetWriter.openFile(schema, nombreArchivo);

        // Escribir datos
        for (const fila of datos) {
            await writer.appendRow(fila);
        }

        await writer.close();
        return true;
    } catch (error) {
        console.log(`Error escribiendo parquet: ${error.message}`);
        return false;
    }
}


async function main() {
    console.log('🔍 Buscando archivos xlsx...');
    
    // Buscar archivos xlsx en directorio actual
    const archivos = fs.readdirSync('.')
        .filter(file => file.endsWith('.xlsx'))
        .map(file => path.resolve(file));
    
    if (archivos.length === 0) {
        console.log('❌ No se encontraron archivos xlsx');
        return;
    }
    
    console.log(`📁 Encontrados ${archivos.length} archivos xlsx`);
    console.log('='.repeat(50));
    
    const todosLosDatos = [];
    let procesados = 0;
    let errores = 0;
    
    // Procesar cada archivo
    archivos.forEach(archivo => {
        const datos = procesarArchivoXlsx(archivo);
        if (datos && datos.length > 0) {
            todosLosDatos.push(...datos);
            procesados++;
        } else {
            errores++;
        }
        console.log('-'.repeat(50));
    });
    
    console.log(`\n📊 Resumen:`);
    console.log(`  ✅ Archivos procesados: ${procesados}`);
    console.log(`  ❌ Archivos con errores: ${errores}`);
    
    if (todosLosDatos.length === 0) {
        console.log('❌ No se obtuvieron datos');
        return;
    }
    
    // Guardar como parquet
    const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const nombreParquet = `datos_combinados_${timestamp}.parquet`;
    
    console.log(`\n🔄 Guardando ${todosLosDatos.length} filas en parquet...`);
    const exito = await escribirParquet(todosLosDatos, nombreParquet);
    
    if (exito) {
        console.log(`\n✅ Proceso completado!`);
        console.log(`📊 Total de filas: ${todosLosDatos.length}`);
        console.log(`📁 Archivo guardado: ${nombreParquet}`);
    } else {
        // Fallback a JSON si parquet falla
        const nombreJson = `datos_combinados_${timestamp}.json`;
        fs.writeFileSync(nombreJson, JSON.stringify(todosLosDatos, null, 2));
        console.log(`\n⚠️ Parquet falló, guardado como JSON: ${nombreJson}`);
    }
    
    // Resumen por partido
    const resumen = {};
    todosLosDatos.forEach(row => {
        const key = `${row.jornada} | ${row.partido}`;
        resumen[key] = (resumen[key] || 0) + 1;
    });
    
    console.log(`\n📈 Resumen por partido:`);
    Object.entries(resumen).forEach(([partido, count]) => {
        console.log(`  ⚽ ${partido}: ${count} filas`);
    });
}

// Verificar si es el archivo principal
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { procesarArchivoXlsx, extraerInfoPartido, escribirParquet };