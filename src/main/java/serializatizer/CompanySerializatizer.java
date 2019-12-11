package serializatizer;

import entity.Company;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.serialization.Serializer;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义的公司序列化器
 */

public class CompanySerializatizer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null){
            return null;
        }
        byte[] name , address;
        try {
            if (data.getName() != null){
                name = data.getName().getBytes("UTF-8");
            }else{
                name = new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            }else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        }catch (Exception e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {
    }
}
