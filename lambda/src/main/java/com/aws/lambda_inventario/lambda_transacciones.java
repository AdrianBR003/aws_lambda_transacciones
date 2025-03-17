package com.aws.lambda_inventario;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class lambda_transacciones implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayProxyResponseEvent> {

    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(Region.US_EAST_1).build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String tableName = "tabla-transacciones";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        try {
            if (request == null) {
                context.getLogger().log("ERROR: La solicitud recibida es NULL");
                return createResponse(500, "Error: La solicitud recibida es NULL");
            }
            String httpMethod = "UNKNOWN";
            if (request.getRequestContext() != null &&
                    request.getRequestContext().getHttp() != null &&
                    request.getRequestContext().getHttp().getMethod() != null) {
                httpMethod = request.getRequestContext().getHttp().getMethod();
            }
            context.getLogger().log("Método HTTP recibido: " + httpMethod);
            switch (httpMethod) {
                case "POST":
                    return createTransaccion(request.getBody(), context);
                case "GET":
                    return getAllTransacciones(context);
                case "DELETE":
                    return deleteTransaccionbyId(request.getBody(), context);
                case "PUT":
                    return modifyTransaccionbyID(request.getBody(), context);
                default:
                    return createResponse(400, "Método HTTP no soportado: " + httpMethod);
            }
        } catch (Exception e) {
            context.getLogger().log("ERROR en handleRequest: " + e.getMessage());
            return createResponse(500, "Error interno: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent modifyTransaccionbyID(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en PUT: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }

            // Convertimos el JSON a un Map
            Map<String, Object> rawMap = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {
            });

            String id_transaccion = (String) rawMap.get("id_transaccion");
            if (id_transaccion == null || id_transaccion.trim().isEmpty()) {
                return createResponse(400, "Falta el campo 'id_transaccion'.");
            }

            // Construimos itemKey solo con la clave primaria
            Map<String, AttributeValue> itemKey = new HashMap<>();
            itemKey.put("id_transaccion", AttributeValue.builder().s(id_transaccion).build());

            // Construimos la expresión de actualización
            StringBuilder updateExpression = new StringBuilder("SET ");
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            Map<String, String> expressionAttributeNames = new HashMap<>();

            int count = 0;
            for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                String key = entry.getKey();
                if (!key.equals("id_transaccion")) {  // Excluimos la clave primaria
                    count++;
                    String fieldKey = "#field" + count;
                    String valueKey = ":val" + count;

                    updateExpression.append(count > 1 ? ", " : "").append(fieldKey).append(" = ").append(valueKey);
                    expressionAttributeNames.put(fieldKey, key);

                    if (entry.getValue() instanceof String) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().s((String) entry.getValue()).build());
                    } else if (entry.getValue() instanceof Number) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().n(entry.getValue().toString()).build());
                    } else if (entry.getValue() instanceof Boolean) {
                        expressionAttributeValues.put(valueKey, AttributeValue.builder().bool((Boolean) entry.getValue()).build());
                    }
                }
            }

            if (count == 0) {
                return createResponse(400, "No hay campos válidos para actualizar.");
            }

            // Construimos la solicitud UpdateItemRequest
            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(itemKey)
                    .updateExpression(updateExpression.toString())
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();

            dynamoDbClient.updateItem(request);
            return createResponse(200, "Transaccion actualizado correctamente.");

        } catch (Exception e) {
            context.getLogger().log("ERROR en modifyTransaccionbyID: " + e.getMessage());
            return createResponse(500, "Error al actualizar transaccion: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent deleteTransaccionbyId(String body, Context context) {
        try{
            context.getLogger().log("Cuerpo recibido en DELETE: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }

            String id_transaccion = "";
            Map<String, Object> bodyMap = objectMapper.readValue(body,new TypeReference<>() {});
            if(body.contains("id_transaccion")){
                id_transaccion = (String) bodyMap.get("id_transaccion");
                System.out.println("Se ha identificado el ID: " + body);
            }else{
                context.getLogger().log("No se ha identificado el ID: " + body);
                System.out.println("No se ha identificado el ID: " + body);
                return createResponse(500, "Error al eliminar transaccion con id" + id_transaccion);
            }

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id_transaccion", AttributeValue.fromS(id_transaccion));
            Object objeto = dynamoDbClient.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(key).returnValues(ReturnValue.ALL_OLD).build());
            context.getLogger().log("Objeto eliminado: " + objeto.toString());
            return createResponse(200, objectMapper.writeValueAsString(bodyMap));
        }catch (Exception e){
            context.getLogger().log("ERROR en deleteTransaccionbyID: " + e.getMessage());
            return createResponse(500, "Error al eliminar transaccion: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createTransaccion(String body, Context context) {
        try {
            context.getLogger().log("Cuerpo recibido en POST: " + body);
            if (body == null || body.trim().isEmpty()) {
                return createResponse(400, "El cuerpo de la solicitud está vacío.");
            }
            Transaccion transaccion = objectMapper.readValue(body, Transaccion.class);
            if (transaccion.id_transaccion == null || transaccion.id_transaccion.trim().isEmpty()) {
                transaccion.setId_transaccion(UUID.randomUUID().toString());
            }
            if (transaccion.getId_transaccion() == null || transaccion.getId_transaccion().trim().isEmpty()) {
                transaccion.setId_transaccion(UUID.randomUUID().toString());
            }
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id_transaccion", AttributeValue.builder().s(transaccion.getId_transaccion()).build());
            if (transaccion.getColeccionOrigen() != null) {
                item.put("coleccionOrigen", AttributeValue.builder().s(transaccion.getColeccionOrigen()).build());
            }
            if (transaccion.getFecha() != null) {
                item.put("coleccionDestino", AttributeValue.builder().s(transaccion.getColeccionDestino()).build());
            }
            if (transaccion.getProducto() != null) {
                item.put("producto", AttributeValue.builder().s(transaccion.getProducto()).build());
            }
            if (transaccion.getColeccionOrigen() != null) {
                item.put("cantidad", AttributeValue.builder().s(String.valueOf(transaccion.getCantidad())).build());
            }
            if (transaccion.getFecha() != null) {
                item.put("fecha", AttributeValue.builder().s(transaccion.getFecha()).build());
            }
            dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
            context.getLogger().log("Transaccion creada con ID: " + transaccion.getId_transaccion());
            return createResponse(200, objectMapper.writeValueAsString(transaccion));
        } catch (Exception e) {
            context.getLogger().log("ERROR en createTransaccion: " + e.getMessage());
            return createResponse(500, "Error al crear transaccion: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent getAllTransacciones(Context context) {
        try {
            context.getLogger().log("Consultando todas las transacciones en la tabla: " + tableName);
            ScanResponse scanResponse = dynamoDbClient.scan(ScanRequest.builder().tableName(tableName).build());
            List<Map<String, AttributeValue>> items = scanResponse.items();
            if (items == null || items.isEmpty()) {
                return createResponse(200, "[]");
            }
            List<Map<String, Object>> convertedItems = items.stream().map(item -> {
                Map<String, Object> map = new HashMap<>();
                item.forEach((key, attributeValue) -> {
                    if (attributeValue.s() != null) {
                        map.put(key, attributeValue.s());
                    } else if (attributeValue.n() != null) {
                        map.put(key, attributeValue.n());
                    } else if (attributeValue.ss() != null && !attributeValue.ss().isEmpty()) {
                        map.put(key, attributeValue.ss());
                    }
                });
                return map;
            }).collect(Collectors.toList());
            String json = objectMapper.writeValueAsString(convertedItems);
            return createResponse(200, json);
        } catch (Exception e) {
            context.getLogger().log("ERROR en getAllTransacciones: " + e.getMessage());
            return createResponse(500, "Error al obtener transacciones: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*"); // Opcional, para CORS
        headers.put("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(headers)
                .withBody(body);
    }

    public static class Transaccion{
        private String id_transaccion;
        private String coleccionOrigen;
        private String coleccionDestino;
        private String producto;
        private int cantidad;
        private String fecha;

        public String getId_transaccion() {
            return id_transaccion;
        }

        public void setId_transaccion(String id_transaccion) {
            this.id_transaccion = id_transaccion;
        }

        public String getColeccionOrigen() {
            return coleccionOrigen;
        }

        public void setColeccionOrigen(String coleccionOrigen) {
            this.coleccionOrigen = coleccionOrigen;
        }

        public String getColeccionDestino() {
            return coleccionDestino;
        }

        public void setColeccionDestino(String coleccionDestino) {
            this.coleccionDestino = coleccionDestino;
        }

        public String getProducto() {
            return producto;
        }

        public void setProducto(String producto) {
            this.producto = producto;
        }

        public int getCantidad() {
            return cantidad;
        }

        public void setCantidad(int cantidad) {
            this.cantidad = cantidad;
        }

        public String getFecha() {
            return fecha;
        }

        public void setFecha(String fecha) {
            this.fecha = fecha;
        }
    }
}
