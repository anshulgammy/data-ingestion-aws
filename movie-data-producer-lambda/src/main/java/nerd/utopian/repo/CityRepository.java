package nerd.utopian.repo;

import java.util.Arrays;
import java.util.List;

public final class CityRepository {

  private static final List<String> CITY_LIST = Arrays.asList(
      "Delhi, Mumbai", "Kolkata", "Chennai", "Bangalore", "Indore", "Patna", "Pune"
  );

  public static final List<String> getCityList() {
    return CITY_LIST;
  }
}
