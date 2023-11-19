package trocaBancoPostgres.teste;

import java.sql.Connection;
import java.sql.SQLException;

import trocaBancoPostgres.conexao.ConexaoNova;

public class TesteConexaoPostgres {

    public static void main(String[] args) {
        Connection conexao = null;
        try {
            conexao = ConexaoNova.obterConexao();
            if (conexao != null) {
                System.out.println("Conexão bem-sucedida!");
            } else {
                System.out.println("Falha na conexão!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conexao != null) {
                try {
                    conexao.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
