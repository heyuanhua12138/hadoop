package com.hyh.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

public class DwdUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    public void process(Object[] objects) throws HiveException {
        JSONArray jsonArray = null;
        try {
            jsonArray = new JSONArray(objects[0].toString());
            if (jsonArray == null || jsonArray.length() == 0) {
                return;
            }
            for (int i = 0; i < jsonArray.length(); i++) {
                String[] forwardStrArr = new String[2];
                try {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    forwardStrArr[0] = jsonObject.getString("en");
                    forwardStrArr[1] = jsonObject.toString();
                } catch (JSONException e) {
                    continue;
                }
                forward(forwardStrArr);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public void close() throws HiveException {

    }
}
