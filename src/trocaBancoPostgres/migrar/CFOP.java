package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class CFOP {

	public static void migrarTabela() {
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = " SELECT id_natureza, desc_natureza, tipo_natureza, "
					+ "usuario_natureza, registro_natureza, id_referencianatureza, stnat\n"
					+ "FROM public.natureza where stnat =1;";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {
					int id = resultSet.getInt("id_referencianatureza");
					String descricao = resultSet.getString("desc_natureza");
					String c = retornaCodigo(descricao);
					System.out.println("c:::" + c + " descrição:::" + descricao);
					salvar(id, c, descricao);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(int idAntigo, String codigo, String descricao) {
		try (Connection novaConexao = ConexaoNova.obterConexao()) {
			String sql = "INSERT INTO CFOP (codigo, descricao, ativo,idAntigo) VALUES (?, ?, true,?)";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {

				preparedStatement.setString(1, codigo);
				preparedStatement.setString(2, descricao);
				preparedStatement.setInt(3, idAntigo);

				preparedStatement.executeUpdate();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void dropTable(String tableName) {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "DROP TABLE " + tableName + " CASCADE";
			statement.executeUpdate(sql);

			System.out.println("Tabela " + tableName + " excluída com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void createTable() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "CREATE TABLE cfop (\n" + "    id SERIAL PRIMARY KEY,\n" + "    codigo VARCHAR(10) NOT NULL,\n"
					+ "    descricao VARCHAR(255) NOT NULL,\n" + "    ativo BOOLEAN NOT NULL,\n"
					+ "    deletado BOOLEAN NOT NULL DEFAULT false\n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela CFOP criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaDeletado() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE cfop\n " + "ALTER COLUMN deletado SET DEFAULT false;";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'CFOP' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaIdAntigo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE CFOP ADD idAntigo int";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'idAntigo' adicionada à tabela 'CFOP' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int encontraCFOP(int idAntingo) throws SQLException {
		Connection conn = ConexaoNova.obterConexao();
		int id = 0;
		try {
			String sql = "SELECT id, codigo, descricao, ativo FROM CFOP WHERE idAntigo = ?";
			PreparedStatement preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, idAntingo);
			ResultSet resultSet = preparedStatement.executeQuery();
			if (resultSet.next()) {
				id = resultSet.getInt("id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		conn.close();
		return id;

	}

	public static String retornaCodigo(String descricao) {
		String codigo = "";
		switch (descricao) {

		case "ENTRADA":
			codigo = "0";
			break;

		case "SAIDA":
			codigo = "1";
			break;

		case "DEVOLUCAO DE CONSIGNACAO":
			codigo = "5405";
			break;
		case "REM. POR CONTA E ORDEM":
			codigo = "5120";
			break;
		case "REMESSA PARA INDUSTRIALIZAÇÃO":
			codigo = "5901";
			break;
		case "OUTRA SAIDA DE MERCADORIA OU SERVICO":
			codigo = "5949";
			break;
		case "SIMPLES REMESSA":
			codigo = "5949";
			break;

		case "VENDA DE MERCCADORIA":
			codigo = "5101";
			break;
		case "REMESSA PARA INDUSTRIALIZACAO":
			codigo = "5949";
			break;
		case "DEVOLUÇÃO MER REC CONSIGNAÇÃO":
			codigo = "5408";
			break;
		case "REMESSA DE MERCADORIA EM CONSIGNACAO":
			codigo = "5403";
			break;

		case "REMESSA P/ DEPÓSITO":
			codigo = "5909";
			break;
		case "REMESSA PARA INDUSTRIALIZACAO POR ENCOMENDA ":
			codigo = "5912";
			break;
		case "RET MERC UTIL NA IND P/ ENCOMENDA":
			codigo = "5958";
			break;
		case "RETORNO DE INDUST. NAO EFETUAD":
			codigo = "5948";
			break;
		case "REM DE BEM POR CONTA E ORDEM CONT COM":
			codigo = "5155";
			break;
		case "REMESSA PARA BENEFICIAMENTO":
			codigo = "5902";
			break;

		case "RETORNO REM P/ INDUSTRIALICAO":
			codigo = "5902";
			break;

		default:
			codigo = "";
			break;
		}
		return codigo;
	}

	public static void main(String[] args) {
		dropTable("CFOP");
		createTable();
		adicionarColunaDeletado();
		adicionarColunaIdAntigo();
		migrarTabela();
	}
}