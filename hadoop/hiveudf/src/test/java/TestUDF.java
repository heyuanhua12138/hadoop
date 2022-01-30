import com.hyh.udf.DwdUTF;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TestUDF {
    public static void main(String[] args) throws JSONException {
        String enentLog = "{1639971148861|{\"cm\":{\"ln\":\"-75.1\",\"sv\":\"V2.2.5\",\"os\":\"8.2.0\",\"g\":\"Z1981HX3@gmail.com\",\"mid\":\"3\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"6\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"3\",\"t\":\"1639882879280\",\"la\":\"-8.5\",\"md\":\"Huawei-11\",\"vn\":\"1.1.4\",\"ba\":\"Huawei\",\"sr\":\"N\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1639967555862\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"3\",\"goodsid\":\"1\",\"news_staytime\":\"36\",\"loading_time\":\"0\",\"action\":\"2\",\"showtype\":\"5\",\"category\":\"43\",\"type1\":\"433\"}},{\"ett\":\"1639917643497\",\"en\":\"ad\",\"kv\":{\"entry\":\"2\",\"show_style\":\"1\",\"action\":\"3\",\"detail\":\"102\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"5\"}}]}";
        DwdUTF dwdUTF = new DwdUTF();
        System.out.println(dwdUTF.evaluate(enentLog,"mid"));
        JSONArray jsonArray = new JSONArray(dwdUTF.evaluate(enentLog, "et"));
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            System.out.println(jsonObject.toString());
        }
    }
}
