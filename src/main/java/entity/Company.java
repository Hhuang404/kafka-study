package entity;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Company {
    private String name;
    private String address;
}
