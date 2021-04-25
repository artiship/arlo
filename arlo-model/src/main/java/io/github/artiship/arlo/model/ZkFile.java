package io.github.artiship.arlo.model;

import com.google.gson.Gson;
import io.github.artiship.arlo.model.enums.FileType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

@Data
@Builder
@AllArgsConstructor
public class ZkFile {
    private static Gson gson = new Gson();
    private String filePath;
    private String fileName;
    private FileType fileType;
    private FileActionType fileActionType;

    public static <T> byte[] toJsonBytes(List<T> asList) {
        return gson.toJson(asList)
                   .getBytes(UTF_8);
    }

    public static ZkFile from(String jsonStr) {
        return gson.fromJson(jsonStr, ZkFile.class);
    }

    public static ZkFile from(byte[] bytes) {
        return from(new String(bytes, UTF_8));
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public String zkPath() {
        return fileName + "-" + fileActionType.getCode();
    }

    public byte[] toJsonBytes() {
        return this.toString()
                   .getBytes(UTF_8);
    }
}
