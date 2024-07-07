package com.kolban.dlp;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;

@SpringBootApplication
public class Deidentify {

  @RestController
  static class AppController {
    @PostMapping("/")
    String deidentify(@RequestBody Map<String, Object> payload
    ) throws Exception {
      //System.out.println("Service called: " + payload);
      Map<String, String> userDefinedContext = (Map<String, String>) payload.get("userDefinedContext");
      if (userDefinedContext == null) {
        throw new Exception("No user defined context");
      }
      if (!userDefinedContext.containsKey("project")) {
        throw new Exception("No project attribute in user defined context");
      }
      if (!userDefinedContext.containsKey("template")) {
        throw new Exception("No template attribute in user defined context");
      }
      // The payload will be
      // calls=[[{name=Bob Smith, salary=50000, ssn=123-45-6789}], [{name=Jane Doe, salary=70000, ssn=987-65-4321}]]
      // This is a list of function calls where each function call is a list of parameters

      Table.Builder tableBuilder = Table.newBuilder();

      // This is where we build the replies
      JSONArray repliesArray = new JSONArray();

      // Extract the list of all function calls.  Since BQ may batch the requests, each list element
      // corresponds to a function call.
      List<List<Map<String, Object>>> allFunctionCallsList = (List<List<Map<String, Object>>>) payload.get("calls");

      Set<String> fieldNames = null; // The set of field names.
      for (List<Map<String, Object>> thisFunctionCallList : allFunctionCallsList) {

        if (!thisFunctionCallList.isEmpty()) {
          Map<String, Object> parameter0 = thisFunctionCallList.get(0);
          // If this is the first parameter .... extract the set of field names.
          if (fieldNames == null) {
            fieldNames = parameter0.keySet();
            for (String key : fieldNames) {
              tableBuilder.addHeaders(FieldId.newBuilder().setName(key).build());
            }
          }

          // Build each table row
          Table.Row.Builder rowBuilder = Table.Row.newBuilder();
          for (String currentField : fieldNames) {
            Object value = parameter0.get(currentField);
            Value.Builder v2Builder = Value.newBuilder();
            if (value instanceof String) {
              v2Builder.setStringValue((String) value);
            } else if (value instanceof Integer) {
              v2Builder.setIntegerValue((Integer) value);
            } else if (value instanceof Boolean) {
              v2Builder.setBooleanValue((Boolean) value);
            } else {
              System.out.println("ERRROR: " + value.getClass());
              v2Builder.setStringValue(value.toString());
            }
            rowBuilder.addValues(v2Builder.build());
          }
          tableBuilder.addRows(rowBuilder.build());
          //repliesArray.put("bob");
        } else { // Replies is empty
          //repliesArray.put((Object) null);
        }
      } // End of for each function call element

      DlpServiceClient dlpServiceClient = DlpServiceClient.create();
      DeidentifyContentRequest request = DeidentifyContentRequest.newBuilder()
        .setParent(ProjectName.of(userDefinedContext.get("project")).toString())
        .setDeidentifyTemplateName(userDefinedContext.get("template"))
        .setItem(ContentItem.newBuilder()
          .setTable(tableBuilder.build())
          .build())
        .build();
      DeidentifyContentResponse response = dlpServiceClient.deidentifyContent(request);
      Table responseTable = response.getItem().getTable();
      List<FieldId> headerList = responseTable.getHeadersList();
      for (Table.Row row : responseTable.getRowsList()) {
        List<Value> valuesList = row.getValuesList();
        JSONObject rowJo = new JSONObject();
        for (int i=0; i<headerList.size(); i++) {
          String fieldName = headerList.get(i).getName();
          Value value = valuesList.get(i);
          if (value.getTypeCase() == Value.TypeCase.STRING_VALUE) {
            rowJo.put(fieldName, value.getStringValue());
          } else if (value.getTypeCase() == Value.TypeCase.INTEGER_VALUE) {
            rowJo.put(fieldName, value.getIntegerValue());
          } else if (value.getTypeCase() == Value.TypeCase.BOOLEAN_VALUE) {
              rowJo.put(fieldName, value.getBooleanValue());
          } else {
            //System.out.println("ERRROR ERRORR!! - Uknown case");
            rowJo.put(fieldName, value.getStringValue());
          }
        }
        repliesArray.put(rowJo);
      }

      //System.out.println(response);
      dlpServiceClient.close();
      JSONObject jo = new JSONObject();
      jo.put("replies", repliesArray);
      //System.out.println(jo.toString());
      return jo.toString();
    }
  } // AppController

  public static void main(String[] args) {
    SpringApplication.run(Deidentify.class, args);
  } // main
} // Deidentify