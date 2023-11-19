package trocaBancoPostgres.conexao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.swing.JOptionPane;

public class ConexaoNova {
    private static final String URL = "jdbc:postgresql://localhost:5432/SysEstoqueSwing2023";
  private static final String USUARIO = "admin";
  private static final String SENHA = "123456";
   public static Connection obterConexao() throws SQLException {
      try {
          return DriverManager.getConnection(URL, USUARIO, SENHA);
      } catch (Exception e) {
          System.out.println("ERRO:\n" + e);
      }
      return null;
  }

  public static void fecharConexao(Connection conexao) {
      if (conexao != null) {
          try {
              conexao.close();
          } catch (SQLException e) {
              JOptionPane.showMessageDialog(null, "Erro ao fechar a conexão: " + e.getMessage());
          }
      }
  }
  
}