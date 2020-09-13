package be.covid.stats.services;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class ComplexKey {
    String key, value;

    public static ComplexKey of(String key, String value) {
        return new ComplexKey(key, value);
    }
}
