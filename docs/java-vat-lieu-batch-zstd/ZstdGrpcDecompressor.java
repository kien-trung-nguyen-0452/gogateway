package vn.softdreams.ebweb.service.reportclickhouse.gateway;

import com.github.luben.zstd.ZstdInputStream;
import io.grpc.Decompressor;
import java.io.IOException;
import java.io.InputStream;

/**
 * Giải nén zstd cho gRPC (grpc-java chỉ có sẵn gzip).
 *
 * <p>Đăng ký vào kênh ở {@link GoGatewayGrpcConfig} với {@code advertised = true}
 * thì client mới gửi {@code grpc-accept-encoding: zstd} và go-gateway mới nén
 * phản hồi bằng zstd. Chỉ có hiệu quả với RPC batch (mỗi message là một lô
 * dòng): nén từng dòng lẻ chỉ được ~1,15x, nén lô vài trăm dòng được ~8-9x.
 */
public final class ZstdGrpcDecompressor implements Decompressor {

    public static final String NAME = "zstd";

    @Override
    public String getMessageEncoding() {
        return NAME;
    }

    @Override
    public InputStream decompress(InputStream in) throws IOException {
        return new ZstdInputStream(in);
    }
}
