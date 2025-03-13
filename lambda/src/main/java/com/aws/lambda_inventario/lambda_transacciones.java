package com.aws.lambda_inventario;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.w3c.dom.Attr;
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
                default:
                    return createResponse(400, "Método HTTP no soportado: " + httpMethod);
            }
        } catch (Exception e) {
            context.getLogger().log("ERROR en handleRequest: " + e.getMessage());
            return createResponse(500, "Error interno: " + e.getMessage());
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
                return createResponse(400, "Falta el campo 'id_transaccion'.");
            }
            if (transaccion.getId_transaccion() == null || transaccion.getId_transaccion().trim().isEmpty()) {
                transaccion.setId_transaccion(UUID.randomUUID().toString());
            }
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id_transaccion", AttributeValue.builder().s(transaccion.getId_transaccion()).build());
            if (transaccion.getId_producto() != null) {
                item.put("id_producto", AttributeValue.builder().s(transaccion.getId_producto()).build());
            }
            if (transaccion.getTipo_movimiento() != null) {
                item.put("tipo_movimiento", AttributeValue.builder().s(transaccion.getTipo_movimiento()).build());
            }
            if (transaccion.getFecha() != null) {
                item.put("fecha", AttributeValue.builder().s(transaccion.getFecha()).build());
            }
            if (transaccion.getCantidad() != null) {
                item.put("cantidad", AttributeValue.builder().s(transaccion.getCantidad()).build());
            }
            if (transaccion.getUbicacion_anterior() != null) {
                item.put("ubicacion_anterior", AttributeValue.builder().s(transaccion.getUbicacion_anterior()).build());
            }
            if (transaccion.getUbicacion_actual() != null) {
                item.put("ubicacion_actual", AttributeValue.builder().s(transaccion.getUbicacion_actual()).build());
            }
            if (transaccion.getResponsable() != null) {
                item.put("responsable", AttributeValue.builder().s(transaccion.getResponsable()).build());
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
        return new APIGatewayProxyResponseEvent().withStatusCode(statusCode).withBody(body);
    }

    public static class Transaccion{
        private String id_transaccion;
        private String id_producto;
        private String tipo_movimiento;
        private String fecha;
        private String cantidad;
        private String ubicacion_anterior;
        private String ubicacion_actual;
        private String responsable;

        public String getId_transaccion() {
            return id_transaccion;
        }

        public void setId_transaccion(String id_transaccion) {
            this.id_transaccion = id_transaccion;
        }

        public String getId_producto() {
            return id_producto;
        }

        public void setId_producto(String id_producto) {
            this.id_producto = id_producto;
        }

        public String getTipo_movimiento() {
            return tipo_movimiento;
        }

        public void setTipo_movimiento(String tipo_movimiento) {
            this.tipo_movimiento = tipo_movimiento;
        }

        public String getFecha() {
            return fecha;
        }

        public void setFecha(String fecha) {
            this.fecha = fecha;
        }

        public String getCantidad() {
            return cantidad;
        }

        public void setCantidad(String cantidad) {
            this.cantidad = cantidad;
        }

        public String getUbicacion_anterior() {
            return ubicacion_anterior;
        }

        public void setUbicacion_anterior(String ubicacion_anterior) {
            this.ubicacion_anterior = ubicacion_anterior;
        }

        public String getUbicacion_actual() {
            return ubicacion_actual;
        }

        public void setUbicacion_actual(String ubicacion_actual) {
            this.ubicacion_actual = ubicacion_actual;
        }

        public String getResponsable() {
            return responsable;
        }

        public void setResponsable(String responsable) {
            this.responsable = responsable;
        }
    }
}
