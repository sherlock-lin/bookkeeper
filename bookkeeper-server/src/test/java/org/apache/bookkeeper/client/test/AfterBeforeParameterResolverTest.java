package org.apache.bookkeeper.client.test;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * @author: shalock.lin date: 2024/10/24 describe:
 */
@ExtendWith(AfterBeforeParameterResolver.class)
public class AfterBeforeParameterResolverTest {
    private TestEnum capturedParameter;
    private Boolean testParam;

    static {

    }

    @BeforeEach
    public void init(Boolean testParam) {
      this.testParam = testParam;
    }

    @ParameterizedTest
    @EnumSource(TestEnum.class)
    public void test2(TestEnum parameter) {
      Assertions.assertThat(parameter).isEqualTo(capturedParameter);
    }

    @ParameterizedTest
    @EnumSource(TestEnum.class)
    public void test(TestEnum parameter) {
      Assertions.assertThat(parameter).isEqualTo(capturedParameter);
    }



    enum TestEnum {
      PARAMETER_1,
      PARAMETER_2
    }
}
