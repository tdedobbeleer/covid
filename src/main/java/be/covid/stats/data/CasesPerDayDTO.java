package be.covid.stats.data;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class CasesPerDayDTO {
    String date;
    Integer total;
}
