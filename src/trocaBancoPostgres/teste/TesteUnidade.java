package trocaBancoPostgres.teste;

import java.sql.SQLException;

import trocaBancoPostgres.migrar.Unidade;

public class TesteUnidade {
	public static void main(String[] args) {
		try {
			System.out.println("teste->" + Unidade.encontraUnidade(2));
		} catch (SQLException e) { 
			e.printStackTrace();
		}
	}

}
