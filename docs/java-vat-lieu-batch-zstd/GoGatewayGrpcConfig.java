package vn.softdreams.ebweb.service.reportclickhouse.gateway;

import io.grpc.DecompressorRegistry;
import net.devh.boot.grpc.client.channelfactory.GrpcChannelConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Khai báo hỗ trợ zstd cho kênh {@code @GrpcClient("go-gateway")}.
 *
 * <p>Kênh này do grpc-client-spring-boot-starter tạo (cấu hình ở
 * {@code grpc.client.go-gateway.*} trong application-*.yml), không phải do code
 * của mình, nên chỉ chỉnh được qua {@link GrpcChannelConfigurer}.
 *
 * <p>Đăng ký decompressor KHÔNG làm đổi hành vi nếu RPC cũ vẫn được gọi: server
 * chỉ nén khi RPC batch bật nén. Client chưa khai báo thì go-gateway vẫn gửi
 * bình thường, không nén.
 */
@Configuration
public class GoGatewayGrpcConfig {

    @Bean
    public GrpcChannelConfigurer goGatewayZstdConfigurer() {
        return (channelBuilder, name) -> {
            if ("go-gateway".equals(name)) {
                channelBuilder.decompressorRegistry(
                    DecompressorRegistry.getDefaultInstance().with(new ZstdGrpcDecompressor(), true));
            }
        };
    }
}
