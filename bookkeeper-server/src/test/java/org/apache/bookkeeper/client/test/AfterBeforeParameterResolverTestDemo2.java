package org.apache.bookkeeper.client.test;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * @author: shalock.lin date: 2024/10/24 describe:
 */
@ExtendWith(AfterBeforeParameterResolver.class)
public class AfterBeforeParameterResolverTestDemo2 {
    private TestEnum capturedParameter;
    private Boolean testParam;

    @BeforeEach
    public void init(Boolean testParam, TestInfo testInfo) {
      this.testParam = testParam;
      System.out.println(testInfo);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void test(Boolean testParam) {
      Assertions.assertThat(testParam).isEqualTo(testParam);
    }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void test2(Boolean testParam) {
    Assertions.assertThat(testParam).isEqualTo(testParam);
  }

    enum TestEnum {
      PARAMETER_1class,
      PARAMETER_2
    }
}
